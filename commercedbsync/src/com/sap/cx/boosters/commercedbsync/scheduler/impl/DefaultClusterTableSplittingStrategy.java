/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler.impl;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.scheduler.ClusterTableSplittingStrategy;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class DefaultClusterTableSplittingStrategy implements ClusterTableSplittingStrategy {
    private static final long DEFAULT_ROW_THRESHOLD = 100000; // Fallback threshold

    protected final MigrationContext migrationContext;

    public DefaultClusterTableSplittingStrategy(MigrationContext migrationContext) {
        this.migrationContext = migrationContext;
    }

    @Override
    public List<Pair<CopyContext.DataCopyItem, Long>> split(CopyContext.DataCopyItem item, Long rowCount,
            int numNodes) {
        List<Pair<CopyContext.DataCopyItem, Long>> virtualItems = new ArrayList<>();
        // doesn't matter if chunking is enabled - we need cluster as well
        Long chunkSize = migrationContext.getClusterChunkSize(item.getTargetItem());
        // global/table chunk size is set (> 0) and there are cluster nodes
        boolean chunkingEnabled = chunkSize > 0 && numNodes > 1;
        // Determine the optimal split size dynamically based on row count and number of
        // nodes
        long splitSize = Math.max(chunkSize,
                (int) Math.ceil(rowCount / numNodes / item.getBatchSize()) * item.getBatchSize());

        // Handle not chunking, empty tables (rowCount == 0) or rowCounts extending
        // splitSize
        if (!chunkingEnabled || rowCount == 0 || splitSize > rowCount) {
            // Create a virtual item for the empty table with rowCount = 0 and no offset
            virtualItems.add(Pair.of(item, rowCount));
            return virtualItems;
        }

        int fullSplits = (int) (rowCount / splitSize);
        long remainingRows = rowCount % splitSize;

        // Create virtual items for each full split
        for (int split = 0; split < fullSplits; split++) {
            CopyContext.DataCopyItem virtualItem = createVirtualItem(item,
                    new CopyContext.DataCopyItem.ChunkData(split, splitSize));
            virtualItems.add(Pair.of(virtualItem, splitSize));
        }

        // Handle the last part with remaining rows, if any
        if (remainingRows > 0) {
            CopyContext.DataCopyItem virtualItem = createVirtualItem(item,
                    new CopyContext.DataCopyItem.ChunkData(fullSplits, splitSize));
            virtualItems.add(Pair.of(virtualItem, remainingRows));
        }

        return virtualItems;
    }

    private CopyContext.DataCopyItem createVirtualItem(CopyContext.DataCopyItem originalItem,
            CopyContext.DataCopyItem.ChunkData chunkData) {
        return new CopyContext.DataCopyItem(originalItem.getSourceItem(), originalItem.getTargetItem(),
                originalItem.getColumnMap(), originalItem.getRowCount(), originalItem.getBatchSize(), chunkData);
    }
}
