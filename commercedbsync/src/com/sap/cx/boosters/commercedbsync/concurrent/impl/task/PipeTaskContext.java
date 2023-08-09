/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl.task;

import com.sap.cx.boosters.commercedbsync.adapter.DataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.concurrent.DataPipe;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceRecorder;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;

public class PipeTaskContext {
    private final CopyContext context;
    private final DataPipe<DataSet> pipe;
    private final String table;
    private final DataRepositoryAdapter dataRepositoryAdapter;
    private final long pageSize;
    private final PerformanceRecorder recorder;
    private final DatabaseCopyTaskRepository taskRepository;

    public PipeTaskContext(CopyContext context, DataPipe<DataSet> pipe, String table,
            DataRepositoryAdapter dataRepositoryAdapter, long pageSize, PerformanceRecorder recorder,
            DatabaseCopyTaskRepository taskRepository) {
        this.context = context;
        this.pipe = pipe;
        this.table = table;
        this.dataRepositoryAdapter = dataRepositoryAdapter;
        this.pageSize = pageSize;
        this.recorder = recorder;
        this.taskRepository = taskRepository;
    }

    public CopyContext getContext() {
        return context;
    }

    public DataPipe<DataSet> getPipe() {
        return pipe;
    }

    public String getTable() {
        return table;
    }

    public DataRepositoryAdapter getDataRepositoryAdapter() {
        return dataRepositoryAdapter;
    }

    public long getPageSize() {
        return pageSize;
    }

    public PerformanceRecorder getRecorder() {
        return recorder;
    }

    public DatabaseCopyTaskRepository getTaskRepository() {
        return taskRepository;
    }
}
