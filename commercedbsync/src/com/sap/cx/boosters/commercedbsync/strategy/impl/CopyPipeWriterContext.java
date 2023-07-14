/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.strategy.impl;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceRecorder;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

class CopyPipeWriterContext {
    private final CopyContext context;
    private final CopyContext.DataCopyItem copyItem;
    private final List<String> columnsToCopy;
    private final Set<String> nullifyColumns;
    private final PerformanceRecorder performanceRecorder;
    private final AtomicLong totalCount;
    private final List<String> upsertIds;
    private final boolean requiresIdentityInsert;
    private final DatabaseCopyTaskRepository databaseCopyTaskRepository;

    public CopyPipeWriterContext(CopyContext context, CopyContext.DataCopyItem copyItem, List<String> columnsToCopy,
            Set<String> nullifyColumns, PerformanceRecorder performanceRecorder, AtomicLong totalCount,
            List<String> upsertIds, boolean requiresIdentityInsert,
            DatabaseCopyTaskRepository databaseCopyTaskRepository) {
        this.context = context;
        this.copyItem = copyItem;
        this.columnsToCopy = columnsToCopy;
        this.nullifyColumns = nullifyColumns;
        this.performanceRecorder = performanceRecorder;
        this.totalCount = totalCount;
        this.upsertIds = upsertIds;
        this.requiresIdentityInsert = requiresIdentityInsert;
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
    }

    public CopyContext getContext() {
        return context;
    }

    public CopyContext.DataCopyItem getCopyItem() {
        return copyItem;
    }

    public List<String> getColumnsToCopy() {
        return columnsToCopy;
    }

    public Set<String> getNullifyColumns() {
        return nullifyColumns;
    }

    public PerformanceRecorder getPerformanceRecorder() {
        return performanceRecorder;
    }

    public AtomicLong getTotalCount() {
        return totalCount;
    }

    public List<String> getUpsertIds() {
        return upsertIds;
    }

    public boolean isRequiresIdentityInsert() {
        return requiresIdentityInsert;
    }

    public DatabaseCopyTaskRepository getDatabaseCopyTaskRepository() {
        return databaseCopyTaskRepository;
    }
}
