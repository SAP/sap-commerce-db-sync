/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl;

import com.google.common.collect.Lists;
import com.sap.cx.boosters.commercedbsync.adapter.DataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.adapter.impl.ContextualDataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.concurrent.DataCopyMethod;
import com.sap.cx.boosters.commercedbsync.concurrent.DataPipe;
import com.sap.cx.boosters.commercedbsync.concurrent.DataPipeFactory;
import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolConfigBuilder;
import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolFactory;
import com.sap.cx.boosters.commercedbsync.concurrent.DataWorkerExecutor;
import com.sap.cx.boosters.commercedbsync.concurrent.MaybeFinished;
import com.sap.cx.boosters.commercedbsync.concurrent.impl.task.*;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceCategory;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceRecorder;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseCopyScheduler;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import com.sap.cx.boosters.commercedbsync.views.TableViewGenerator;

import de.hybris.platform.core.Registry;
import org.apache.commons.lang3.tuple.Pair;
import org.fest.util.Collections;
import com.sap.cx.boosters.commercedbsync.DataThreadPoolConfig;
import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DefaultDataPipeFactory implements DataPipeFactory<DataSet> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataPipeFactory.class);

    private final DatabaseCopyTaskRepository taskRepository;
    private final DatabaseCopyScheduler scheduler;
    private final AsyncTaskExecutor executor;
    private final DataThreadPoolFactory dataReadWorkerPoolFactory;

    public DefaultDataPipeFactory(DatabaseCopyScheduler scheduler, DatabaseCopyTaskRepository taskRepository,
            AsyncTaskExecutor executor, DataThreadPoolFactory dataReadWorkerPoolFactory) {
        this.scheduler = scheduler;
        this.taskRepository = taskRepository;
        this.executor = executor;
        this.dataReadWorkerPoolFactory = dataReadWorkerPoolFactory;
    }

    @Override
    public DataPipe<DataSet> create(CopyContext context, CopyContext.DataCopyItem item) throws Exception {
        int dataPipeTimeout = context.getMigrationContext().getDataPipeTimeout();
        int dataPipeCapacity = context.getMigrationContext().getDataPipeCapacity();
        DataPipe<DataSet> pipe = new DefaultDataPipe<>(scheduler, taskRepository, context, item, dataPipeTimeout,
                dataPipeCapacity);
        DataThreadPoolConfig threadPoolConfig = new DataThreadPoolConfigBuilder(context.getMigrationContext())
                .withPoolSize(context.getMigrationContext().getMaxParallelReaderWorkers()).build();
        final ThreadPoolTaskExecutor taskExecutor = dataReadWorkerPoolFactory.create(context, threadPoolConfig);
        DataWorkerExecutor<Boolean> workerExecutor = new DefaultDataWorkerExecutor<>(taskExecutor);
        try {
            executor.submit(() -> {
                try {
                    if (!Registry.hasCurrentTenant()) {
                        Registry.activateMasterTenant();
                    }
                    scheduleWorkers(context, workerExecutor, pipe, item);
                    workerExecutor.waitAndRethrowUncaughtExceptions();
                    pipe.put(MaybeFinished.finished(DataSet.EMPTY));
                } catch (Exception e) {
                    LOG.error("Error scheduling worker tasks ", e);
                    try {
                        pipe.put(MaybeFinished.poison());
                    } catch (Exception p) {
                        LOG.error("Could not close contaminated pipe ", p);
                    }
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                } finally {
                    if (taskExecutor != null) {
                        dataReadWorkerPoolFactory.destroy(taskExecutor);
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Error invoking reader tasks ", e);
        }
        return pipe;
    }

    private static String getRecorderName(CopyContext.DataCopyItem copyItem, boolean chunkedTable) {
        return copyItem.getSourceItem() + (chunkedTable ? copyItem.getChunkData().getCurrentChunk() : "");
    }

    private void scheduleWorkers(CopyContext context, DataWorkerExecutor<Boolean> workerExecutor,
            DataPipe<DataSet> pipe, CopyContext.DataCopyItem copyItem) throws Exception {
        DataRepositoryAdapter dataRepositoryAdapter = new ContextualDataRepositoryAdapter(
                context.getMigrationContext().getDataSourceRepository());
        String table = copyItem.getSourceItem();
        long totalRows = copyItem.getRowCount();
        int batchSize = copyItem.getBatchSize();
        boolean chunkedTable = copyItem.getChunkData() != null;
        try {
            PerformanceRecorder recorder = context.getPerformanceProfiler().createRecorder(PerformanceCategory.DB_READ,
                    getRecorderName(copyItem, chunkedTable));
            recorder.start();

            PipeTaskContext pipeTaskContext = new PipeTaskContext(context, pipe, table, dataRepositoryAdapter,
                    batchSize, recorder, taskRepository);

            String batchColumn = "";
            // help.sap.com/viewer/d0224eca81e249cb821f2cdf45a82ace/LATEST/en-US/08a27931a21441b59094c8a6aa2a880e.html
            final Set<String> allColumnNames = context.getMigrationContext().getDataSourceRepository()
                    .getAllColumnNames(table);
            if (allColumnNames.contains("PK")) {
                batchColumn = "PK";
            } else if (allColumnNames.contains("ID")
                    && context.getMigrationContext().getDataSourceRepository().isAuditTable(table)) {
                batchColumn = "ID";
            }
            LOG.debug("Using batchColumn: {}", batchColumn.isEmpty() ? "NONE" : batchColumn);

            if (batchColumn.isEmpty()) {
                // trying offset queries with unique index columns
                Set<String> batchColumns = getBatchColumns(context, table);
                if (!batchColumns.isEmpty()) {
                    taskRepository.updateTaskCopyMethod(context, copyItem, DataCopyMethod.OFFSET.toString());
                    taskRepository.updateTaskKeyColumns(context, copyItem, batchColumns);

                    List<Pair<Long, Long>> batches;
                    if (context.getMigrationContext().isSchedulerResumeEnabled()) {
                        Set<DatabaseCopyBatch> pendingBatchesForPipeline = taskRepository
                                .findPendingBatchesForPipeline(context, copyItem);
                        batches = pendingBatchesForPipeline.stream()
                                .map(b -> Pair.of(Long.valueOf(b.getLowerBoundary().toString()),
                                        Long.valueOf(b.getUpperBoundary().toString())))
                                .collect(Collectors.toList());
                        taskRepository.resetPipelineBatches(context, copyItem);
                    } else {
                        batches = new ArrayList<>();
                        long chunkOffset = chunkedTable
                                ? copyItem.getChunkData().getChunkSize() * copyItem.getChunkData().getCurrentChunk()
                                : 0;
                        for (long offset = 0; offset < totalRows; offset += batchSize) {
                            batches.add(Pair.of(chunkOffset + offset, chunkOffset + offset + batchSize));
                        }
                    }

                    Pair<Long, Long> boundaries;
                    for (int batchId = 0; batchId < batches.size(); batchId++) {
                        boundaries = batches.get(batchId);
                        DataReaderTask dataReaderTask = new BatchOffsetDataReaderTask(pipeTaskContext, batchId,
                                boundaries.getLeft(), batchColumns);
                        taskRepository.scheduleBatch(context, copyItem, batchId, boundaries.getLeft(),
                                boundaries.getRight());
                        workerExecutor.safelyExecute(dataReaderTask);
                    }
                } else {
                    // If no unique columns available to do batch sorting, fallback to read all
                    LOG.warn(
                            "Reading all rows at once without batching for table {}. Memory consumption might be negatively affected",
                            table);
                    taskRepository.updateTaskCopyMethod(context, copyItem, DataCopyMethod.DEFAULT.toString());
                    if (context.getMigrationContext().isSchedulerResumeEnabled()) {
                        taskRepository.resetPipelineBatches(context, copyItem);
                    }
                    taskRepository.scheduleBatch(context, copyItem, 0, 0, totalRows);
                    DataReaderTask dataReaderTask = new DefaultDataReaderTask(pipeTaskContext);
                    workerExecutor.safelyExecute(dataReaderTask);
                }
            } else {
                // do the pagination by value comparison
                taskRepository.updateTaskCopyMethod(context, copyItem, DataCopyMethod.SEEK.toString());
                taskRepository.updateTaskKeyColumns(context, copyItem, Lists.newArrayList(batchColumn));
                List<List<Object>> batchMarkersList;
                if (context.getMigrationContext().isSchedulerResumeEnabled()) {
                    if (context.getMigrationContext().getPartitionedTables().contains(table)) {
                        LOG.debug("Resuming partitioned table {}", table);
                        final var partitions = dataRepositoryAdapter.getPartitions(table);
                        for (String partition : partitions) {
                            Set<DatabaseCopyBatch> pendingBatchesForPipeline = taskRepository
                                    .findPendingBatchesForPipeline(context, copyItem, partition);
                            batchMarkersList = new ArrayList<>(pendingBatchesForPipeline.stream()
                                    .map(b -> Collections.list(b.getLowerBoundary())).toList());
                            taskRepository.resetPipelineBatches(context, copyItem, partition);
                            createDataReaderTasks(workerExecutor, pipeTaskContext, batchColumn, batchMarkersList,
                                    copyItem, chunkedTable, partition);
                        }
                    } else {
                        Set<DatabaseCopyBatch> pendingBatchesForPipeline = taskRepository
                                .findPendingBatchesForPipeline(context, copyItem);
                        batchMarkersList = pendingBatchesForPipeline.stream()
                                .map(b -> Collections.list(b.getLowerBoundary())).collect(Collectors.toList());
                        taskRepository.resetPipelineBatches(context, copyItem);
                        createDataReaderTasks(workerExecutor, pipeTaskContext, batchColumn, batchMarkersList, copyItem,
                                chunkedTable, null);
                    }
                } else {
                    if (context.getMigrationContext().getPartitionedTables().contains(table)) {
                        LOG.debug("Processing partitioned table {}", table);
                        final var partitions = dataRepositoryAdapter.getPartitions(table);
                        for (String partition : partitions) {
                            MarkersQueryDefinition queryDefinition = new MarkersQueryDefinition();
                            queryDefinition.setPartition(partition);
                            LOG.debug("getBatchMarkers for partition {}", partition);
                            final var batchMarkers = getBatchMarkers(context, dataRepositoryAdapter, table, batchSize,
                                    batchColumn, queryDefinition);
                            createDataReaderTasks(workerExecutor, pipeTaskContext, batchColumn, batchMarkers, copyItem,
                                    chunkedTable, partition);
                        }
                    } else {
                        MarkersQueryDefinition queryDefinition = new MarkersQueryDefinition();
                        final var batchMarkers = getBatchMarkers(context, dataRepositoryAdapter, table, batchSize,
                                batchColumn, queryDefinition);
                        createDataReaderTasks(workerExecutor, pipeTaskContext, batchColumn, batchMarkers, copyItem,
                                chunkedTable, null);
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("{{}}: Exception while preparing reader tasks", table, ex);
            pipe.requestAbort(ex);
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Exception while preparing reader tasks", ex);
        }
    }

    private static boolean isCurrentChunkBatch(CopyContext.DataCopyItem copyItem, boolean chunkedTable, int i) {
        boolean processBatch = true;
        if (chunkedTable) {
            // if chunked data process only batch items that belong to this batch
            CopyContext.DataCopyItem.ChunkData chunkData = copyItem.getChunkData();
            int batchesPerChunk = (int) (chunkData.getChunkSize() / copyItem.getBatchSize());
            processBatch = (i >= chunkData.getCurrentChunk() * batchesPerChunk)
                    && (i < (chunkData.getCurrentChunk() + 1) * batchesPerChunk);
        }
        return processBatch;
    }

    protected List<List<Object>> getBatchMarkers(CopyContext context, DataRepositoryAdapter dataRepositoryAdapter,
            String table, long batchSize, String batchColumn, MarkersQueryDefinition queryDefinition) throws Exception {
        queryDefinition.setTable(table);
        queryDefinition.setColumn(batchColumn);
        queryDefinition.setBatchSize(batchSize);
        queryDefinition.setDeletionEnabled(context.getMigrationContext().isDeletionEnabled());
        queryDefinition.setLpTableEnabled(context.getMigrationContext().isLpTableMigrationEnabled());
        DataSet batchMarkers = dataRepositoryAdapter.getBatchMarkersOrderedByColumn(context.getMigrationContext(),
                queryDefinition);
        List<List<Object>> batchMarkersList = batchMarkers.getAllResults();
        if (batchMarkersList.isEmpty()) {
            throw new RuntimeException("Could not retrieve batch values for table " + table);
        }
        return batchMarkersList;
    }

    protected void createDataReaderTasks(DataWorkerExecutor<Boolean> workerExecutor, PipeTaskContext pipeTaskContext,
            String batchColumn, List<List<Object>> batchMarkersList, final CopyContext.DataCopyItem copyItem,
            final boolean chunkedTable, String partition) throws Exception {
        for (int i = 0; i < batchMarkersList.size(); i++) {
            boolean processBatch = isCurrentChunkBatch(copyItem, chunkedTable, i);
            if (processBatch) {
                List<Object> lastBatchMarkerRow = batchMarkersList.get(i);
                Optional<List<Object>> nextBatchMarkerRow = Optional.empty();
                int nextIndex = i + 1;
                if (nextIndex < batchMarkersList.size()) {
                    nextBatchMarkerRow = Optional.of(batchMarkersList.get(nextIndex));
                }
                if (!Collections.isEmpty(lastBatchMarkerRow)) {
                    Object lastBatchValue = lastBatchMarkerRow.get(0);
                    Object nextValue = nextBatchMarkerRow.map(v -> v.get(0)).orElseGet(() -> null);
                    // check if nextValue is null and allow Pair(value, null) only if it is last
                    // chunk
                    Pair<Object, Object> batchMarkersPair = Pair.of(lastBatchValue, nextValue);
                    DataReaderTask dataReaderTask = partition == null
                            ? new BatchMarkerDataReaderTask(pipeTaskContext, i, batchColumn, batchMarkersPair, false)
                            : new PartitionedBatchMarkerDataReaderTask(pipeTaskContext, i, batchColumn,
                                    batchMarkersPair, false, partition);
                    // After creating the task, we register the batch in the db for later use if
                    // necessary
                    taskRepository.scheduleBatch(pipeTaskContext.getContext(), copyItem, i, batchMarkersPair.getLeft(),
                            batchMarkersPair.getRight(), partition);
                    workerExecutor.safelyExecute(dataReaderTask);
                } else {
                    throw new IllegalArgumentException("Invalid batch marker passed to task");
                }
            }
        }
    }

    protected Set<String> getBatchColumns(CopyContext context, String table) throws Exception {
        if (context.getMigrationContext().getBatchColumns().containsKey(table)) {
            return context.getMigrationContext().getBatchColumns().get(table);
        } else {
            DataSet uniqueColumns = context.getMigrationContext().getDataSourceRepository()
                    .getUniqueColumns(TableViewGenerator.getTableNameForView(table, context.getMigrationContext()));
            if (uniqueColumns.isNotEmpty()) {
                if (uniqueColumns.getColumnCount() == 0) {
                    throw new IllegalStateException(
                            "Corrupt dataset retrieved. Dataset should have information about unique columns");
                }
                return uniqueColumns.getAllResults().stream().map(row -> String.valueOf(row.get(0)))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
            }
            return Set.of();
        }
    }
}
