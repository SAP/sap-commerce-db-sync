/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl.task;

import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import org.apache.commons.lang3.tuple.Pair;

public class PartitionedBatchMarkerDataReaderTask extends BatchMarkerDataReaderTask {
    private final String partition;

    public PartitionedBatchMarkerDataReaderTask(PipeTaskContext pipeTaskContext, int batchId, String batchColumn,
            Pair<Object, Object> batchMarkersPair, boolean upperBoundInclusive, String partition) {
        super(pipeTaskContext, batchId, batchColumn, batchMarkersPair, upperBoundInclusive);
        this.partition = partition;
    }

    @Override
    protected SeekQueryDefinition createSeekQueryDefinition(final Object lastValue, final Object nextValue,
            final String table, final long pageSize, final CopyContext ctx) {
        final var seekQueryDefinition = super.createSeekQueryDefinition(lastValue, nextValue, table, pageSize, ctx);
        seekQueryDefinition.setPartition(getPartition());
        return seekQueryDefinition;
    }

    public String getPartition() {
        return partition;
    }
}
