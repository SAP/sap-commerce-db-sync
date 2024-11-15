/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl.task;

import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import com.sap.cx.boosters.commercedbsync.adapter.DataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.concurrent.MaybeFinished;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchMarkerDataReaderTask extends DataReaderTask {
    private static final Logger LOG = LoggerFactory.getLogger(BatchMarkerDataReaderTask.class);

    private final String batchColumn;
    private final Pair<Object, Object> batchMarkersPair;
    private final int batchId;
    private final boolean upperBoundInclusive;

    public BatchMarkerDataReaderTask(PipeTaskContext pipeTaskContext, int batchId, String batchColumn,
            Pair<Object, Object> batchMarkersPair, boolean upperBoundInclusive) {
        super(pipeTaskContext);
        this.batchId = batchId;
        this.batchColumn = batchColumn;
        this.batchMarkersPair = batchMarkersPair;
        this.upperBoundInclusive = upperBoundInclusive;
    }

    @Override
    protected Boolean internalRun() throws Exception {
        waitForFreeMemory();
        process(batchMarkersPair.getLeft(), batchMarkersPair.getRight());
        return Boolean.TRUE;
    }

    private void process(Object lastValue, Object nextValue) throws Exception {
        CopyContext ctx = getPipeTaskContext().getContext();
        DataRepositoryAdapter adapter = getPipeTaskContext().getDataRepositoryAdapter();
        String table = getPipeTaskContext().getTable();
        long pageSize = getPipeTaskContext().getPageSize();
        SeekQueryDefinition queryDefinition = new SeekQueryDefinition();
        queryDefinition.setBatchId(batchId);
        queryDefinition.setTable(table);
        queryDefinition.setColumn(batchColumn);
        queryDefinition.setLastColumnValue(lastValue);
        queryDefinition.setNextColumnValue(nextValue);
        queryDefinition.setBatchSize(pageSize);
        queryDefinition.setDeletionEnabled(ctx.getMigrationContext().isDeletionEnabled());
        queryDefinition.setLpTableEnabled(ctx.getMigrationContext().isLpTableMigrationEnabled());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing markers query for {} with lastvalue: {}, nextvalue: {}, batchsize: {}", table,
                    lastValue, nextValue, pageSize);
        }
        DataSet page = adapter.getBatchOrderedByColumn(ctx.getMigrationContext(), queryDefinition);

        profileData(ctx, batchId, table, pageSize, page);

        getPipeTaskContext().getRecorder().record(PerformanceUnit.ROWS, pageSize);
        getPipeTaskContext().getPipe().put(MaybeFinished.of(page));
    }
}
