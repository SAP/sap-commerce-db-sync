/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl;

import com.sap.cx.boosters.commercedbsync.concurrent.DataPipe;
import com.sap.cx.boosters.commercedbsync.concurrent.MaybeFinished;
import com.sap.cx.boosters.commercedbsync.concurrent.PipeAbortedException;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseCopyScheduler;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultDataPipe<T> implements DataPipe<T> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataPipe.class);

    private final BlockingQueue<MaybeFinished<T>> queue;
    private final int defaultTimeout;
    private final AtomicReference<Exception> abortException = new AtomicReference<>();
    private final AtomicInteger size = new AtomicInteger();
    private final CopyContext context;
    private final CopyContext.DataCopyItem copyItem;
    private final DatabaseCopyTaskRepository taskRepository;
    private final DatabaseCopyScheduler scheduler;

    public DefaultDataPipe(DatabaseCopyScheduler scheduler, DatabaseCopyTaskRepository taskRepository,
            CopyContext context, CopyContext.DataCopyItem copyItem, int timeoutInSeconds, int capacity) {
        this.taskRepository = taskRepository;
        this.scheduler = scheduler;
        this.context = context;
        this.copyItem = copyItem;
        this.queue = new ArrayBlockingQueue<>(capacity);
        defaultTimeout = timeoutInSeconds;
    }

    @Override
    public void requestAbort(Exception cause) {
        if (this.abortException.compareAndSet(null, cause)) {
            if (context.getMigrationContext().isFailOnErrorEnabled()) {
                try {
                    scheduler.abort(context);
                } catch (Exception ex) {
                    LOG.warn("could not abort", ex);
                }
            }
            try {
                taskRepository.markTaskFailed(context, copyItem, cause);
            } catch (Exception e) {
                LOG.warn("could not update error status!", e);
            }
            try {
                flushPipe();
            } catch (Exception e) {
                LOG.warn("Could not flush pipe", e);
            }
        }
    }

    private void flushPipe() throws Exception {
        // make sure waiting queue offers can be flushed
        while (getWaitersCount() > 0) {
            queue.poll(defaultTimeout, TimeUnit.SECONDS);
            size.decrementAndGet();
        }
        queue.clear();
    }

    private boolean isAborted() throws Exception {
        if (this.abortException.get() == null && scheduler.isAborted(this.context)) {
            requestAbort(new PipeAbortedException("Migration aborted"));
        }
        return this.abortException.get() != null;
    }

    private void assertPipeNotAborted() throws Exception {
        if (isAborted()) {
            throw new PipeAbortedException("Pipe aborted", this.abortException.get());
        }
    }

    @Override
    public void put(MaybeFinished<T> value) throws Exception {
        assertPipeNotAborted();
        size.incrementAndGet();
        if (!queue.offer(value, defaultTimeout, TimeUnit.SECONDS)) {
            throw new RuntimeException("cannot put new item in time");
        }
    }

    @Override
    public MaybeFinished<T> get() throws Exception {
        assertPipeNotAborted();
        MaybeFinished<T> element = queue.poll(defaultTimeout, TimeUnit.SECONDS);
        size.decrementAndGet();
        if (element == null) {
            throw new RuntimeException(String.format(
                    "cannot get new item in time. Consider increasing the value of the property '%s' or '%s'",
                    CommercedbsyncConstants.MIGRATION_DATA_PIPE_TIMEOUT,
                    CommercedbsyncConstants.MIGRATION_DATA_PIPE_CAPACITY));
        }
        return element;
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public int getWaitersCount() {
        return size.get() - queue.size();
    }
}
