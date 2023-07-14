/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.google.common.base.Stopwatch;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseCopyScheduler;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationCopyService;
import com.sap.cx.boosters.commercedbsync.strategy.PipeWriterStrategy;
import org.apache.commons.lang3.tuple.Pair;
import com.sap.cx.boosters.commercedbsync.DataThreadPoolConfig;
import com.sap.cx.boosters.commercedbsync.concurrent.DataPipe;
import com.sap.cx.boosters.commercedbsync.concurrent.DataPipeFactory;
import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolConfigBuilder;
import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolFactory;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Service to start the asynchronous migration
 */
public class PipeDatabaseMigrationCopyService implements DatabaseMigrationCopyService {
    private static final Logger LOG = LoggerFactory.getLogger(PipeDatabaseMigrationCopyService.class);

    private final DataPipeFactory<DataSet> pipeFactory;
    private final PipeWriterStrategy<DataSet> writerStrategy;
    private final DataThreadPoolFactory dataWriteTaskFactory;
    private final DatabaseCopyTaskRepository databaseCopyTaskRepository;
    private final DatabaseCopyScheduler scheduler;

    public PipeDatabaseMigrationCopyService(DataPipeFactory<DataSet> pipeFactory,
            PipeWriterStrategy<DataSet> writerStrategy, DataThreadPoolFactory dataWriteTaskFactory,
            DatabaseCopyTaskRepository databaseCopyTaskRepository, DatabaseCopyScheduler scheduler) {
        this.pipeFactory = pipeFactory;
        this.writerStrategy = writerStrategy;
        this.dataWriteTaskFactory = dataWriteTaskFactory;
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
        this.scheduler = scheduler;
    }

    @Override
    public void copyAllAsync(CopyContext context) {
        Set<CopyContext.DataCopyItem> copyItems = context.getCopyItems();
        Deque<Pair<CopyContext.DataCopyItem, Callable<Boolean>>> tasksToSchedule = generateCopyTasks(context,
                copyItems);
        scheduleTasks(context, tasksToSchedule);
    }

    /**
     * Creates Tasks to copy the Data
     *
     * @param context
     * @param copyItems
     * @return
     */
    private Deque<Pair<CopyContext.DataCopyItem, Callable<Boolean>>> generateCopyTasks(CopyContext context,
            Set<CopyContext.DataCopyItem> copyItems) {
        return copyItems.stream().map(item -> Pair.of(item, (Callable<Boolean>) () -> {
            final Stopwatch timer = Stopwatch.createStarted();
            try (MDC.MDCCloseable ignored = MDC.putCloseable(CommercedbsyncConstants.MDC_PIPELINE,
                    item.getPipelineName())) {
                try {
                    copy(context, item);
                } catch (Exception e) {
                    LOG.error("Failed to copy item", e);
                    return Boolean.FALSE;
                } finally {
                    final Stopwatch endStop = timer.stop();
                    silentlyUpdateCompletedState(context, item, endStop.toString(), endStop.elapsed().getSeconds());
                }
            }
            return Boolean.TRUE;
        })).collect(Collectors.toCollection(LinkedList::new));
    }

    /**
     * Performs the actual copy of an item
     *
     * @param copyContext
     * @param item
     * @throws Exception
     */
    private void copy(CopyContext copyContext, CopyContext.DataCopyItem item) throws Exception {
        DataPipe<DataSet> dataPipe = null;
        try {
            dataPipe = pipeFactory.create(copyContext, item);
            writerStrategy.write(copyContext, dataPipe, item);
        } catch (Exception e) {
            if (dataPipe != null) {
                dataPipe.requestAbort(e);
            }
            throw e;
        }
    }

    /**
     * Adds the tasks to the executor
     *
     * @param context
     * @param tasksToSchedule
     */
    private void scheduleTasks(CopyContext context,
            Deque<Pair<CopyContext.DataCopyItem, Callable<Boolean>>> tasksToSchedule) {
        List<Pair<CopyContext.DataCopyItem, Future<Boolean>>> runningTasks = new ArrayList<>();
        BackOffExecution backoff = null;
        CopyContext.DataCopyItem previousReject = null;
        DataThreadPoolConfig poolConfig = new DataThreadPoolConfigBuilder(context.getMigrationContext())
                .withPoolSize(context.getMigrationContext().getMaxParallelTableCopy()).build();
        ThreadPoolTaskExecutor executor = dataWriteTaskFactory.create(context, poolConfig);
        try {
            while (tasksToSchedule.peekFirst() != null) {
                Pair<CopyContext.DataCopyItem, Callable<Boolean>> task = tasksToSchedule.removeFirst();
                try {
                    runningTasks.add(Pair.of(task.getLeft(), executor.submit(task.getRight())));
                } catch (TaskRejectedException e) {
                    // this shouldn't really happen, the writer thread pool has an unbounded queue
                    // but better be safe than sorry...
                    tasksToSchedule.addFirst(task);
                    if (!Objects.equals(task.getLeft(), previousReject)) {
                        backoff = new ExponentialBackOff().start();
                    }
                    if (backoff != null) {
                        previousReject = task.getLeft();
                        long waitInterval = backoff.nextBackOff();
                        LOG.debug("Task rejected. Retrying in {}ms...", waitInterval);
                        Thread.sleep(waitInterval);
                    }
                }
            }
            // all tasks submitted, graceful shutdown
            dataWriteTaskFactory.destroy(executor);
        } catch (Exception e) {
            try {
                scheduler.abort(context);
            } catch (Exception exception) {
                LOG.error("Could not abort migration", e);
            }
            for (Pair<CopyContext.DataCopyItem, Future<Boolean>> running : runningTasks) {
                if (running.getRight().cancel(true)) {
                    markAsCancelled(context, running.getLeft());
                }
            }
            for (Pair<CopyContext.DataCopyItem, Callable<Boolean>> copyTask : tasksToSchedule) {
                markAsCancelled(context, copyTask.getLeft());
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }

        LOG.debug("Running Tasks" + runningTasks.size());
    }

    private void markAsCancelled(CopyContext context, CopyContext.DataCopyItem item) {
        try {
            databaseCopyTaskRepository.markTaskFailed(context, item, new RuntimeException("Execution cancelled"));
        } catch (Exception e) {
            LOG.error("Failed to set cancelled status", e);
        }
    }

    private void silentlyUpdateCompletedState(final CopyContext context, final CopyContext.DataCopyItem item,
            final String duration, final float durationSeconds) {
        try {
            databaseCopyTaskRepository.markTaskCompleted(context, item, duration, durationSeconds);
        } catch (final Exception e) {
            LOG.error("Failed to update copy status", e);
        }
    }
}
