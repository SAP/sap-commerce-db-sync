/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl.task;

import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.adapter.DataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.concurrent.MaybeFinished;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceUnit;

import java.util.Set;
import java.util.stream.Collectors;

public class BatchOffsetDataReaderTask extends DataReaderTask {

    private final long offset;
    private final Set<String> batchColumns;
    private final int batchId;

    public BatchOffsetDataReaderTask(PipeTaskContext pipeTaskContext, int batchId, long offset,
            Set<String> batchColumns) {
        super(pipeTaskContext);
        this.batchId = batchId;
        this.offset = offset;
        this.batchColumns = batchColumns;
    }

    @Override
    protected Boolean internalRun() throws Exception {
        process();
        return Boolean.TRUE;
    }

    private void process() throws Exception {
        DataRepositoryAdapter adapter = getPipeTaskContext().getDataRepositoryAdapter();
        CopyContext context = getPipeTaskContext().getContext();
        String table = getPipeTaskContext().getTable();
        long pageSize = getPipeTaskContext().getPageSize();
        OffsetQueryDefinition queryDefinition = new OffsetQueryDefinition();
        queryDefinition.setBatchId(batchId);
        queryDefinition.setTable(table);
        queryDefinition.setOrderByColumns(batchColumns.stream().collect(Collectors.joining(",")));
        queryDefinition.setBatchSize(pageSize);
        queryDefinition.setOffset(offset);
        queryDefinition.setDeletionEnabled(context.getMigrationContext().isDeletionEnabled());
        queryDefinition.setLpTableEnabled(context.getMigrationContext().isLpTableMigrationEnabled());
        DataSet result = adapter.getBatchWithoutIdentifier(context.getMigrationContext(), queryDefinition);
        getPipeTaskContext().getRecorder().record(PerformanceUnit.ROWS, result.getAllResults().size());
        getPipeTaskContext().getPipe().put(MaybeFinished.of(result));
    }
}
