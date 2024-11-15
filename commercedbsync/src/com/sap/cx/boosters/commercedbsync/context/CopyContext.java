/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context;

import com.sap.cx.boosters.commercedbsync.performance.PerformanceProfiler;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;

/**
 * Contains the Information needed to Copy Data
 */
public class CopyContext {

    private final String migrationId;
    private final MigrationContext migrationContext;
    private final Set<DataCopyItem> copyItems;
    private final PerformanceProfiler performanceProfiler;
    private final Map<String, Serializable> propertyOverrideMap;

    public CopyContext(String migrationId, MigrationContext migrationContext, Set<DataCopyItem> copyItems,
            PerformanceProfiler performanceProfiler) {
        this.migrationId = migrationId;
        this.migrationContext = migrationContext;
        this.copyItems = copyItems;
        this.performanceProfiler = performanceProfiler;
        this.propertyOverrideMap = new HashMap<>();
    }

    public IdCopyContext toIdCopyContext() {
        return new IdCopyContext(migrationId, migrationContext, performanceProfiler);
    }

    public MigrationContext getMigrationContext() {
        return migrationContext;
    }

    /**
     * Media Items to be Copied
     *
     * @return
     */
    public Set<DataCopyItem> getCopyItems() {
        return copyItems;
    }

    public String getMigrationId() {
        return migrationId;
    }

    public PerformanceProfiler getPerformanceProfiler() {
        return performanceProfiler;
    }

    public Map<String, Serializable> getPropertyOverrideMap() {
        return propertyOverrideMap;
    }

    public static class DataCopyItem {
        private String sourceItem;
        private final String targetItem;
        private final Map<String, String> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        private final Long rowCount;
        private final Integer batchSize;
        private final ChunkData chunkData;

        public DataCopyItem(String sourceItem, String targetItem, Integer batchSize) {
            this.sourceItem = sourceItem;
            this.targetItem = targetItem;
            this.batchSize = batchSize;
            this.rowCount = null;
            this.chunkData = null;
        }

        public DataCopyItem(String sourceItem, String targetItem, Map<String, String> columnMap, Long rowCount,
                Integer batchSize, ChunkData chunkData) {
            this.sourceItem = sourceItem;
            this.targetItem = targetItem;
            this.batchSize = batchSize;
            this.columnMap.clear();
            this.columnMap.putAll(columnMap);
            this.rowCount = rowCount;
            this.chunkData = chunkData;
        }

        public String getSourceItem() {
            return sourceItem;
        }

        public void setSourceItem(String sourceItem) {
            this.sourceItem = sourceItem;
        }

        public String getTargetItem() {
            return targetItem;
        }

        public String getPipelineName() {
            return getSourceItem() + (this.chunkData != null ? this.chunkData.getCurrentChunk() : "") + "->"
                    + getTargetItem();
        }

        public Map<String, String> getColumnMap() {
            return columnMap;
        }

        public Long getRowCount() {
            return rowCount;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public ChunkData getChunkData() {
            return chunkData;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", DataCopyItem.class.getSimpleName() + "[", "]")
                    .add("sourceItem='" + sourceItem + "'").add("targetItem='" + targetItem + "'").toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            DataCopyItem that = (DataCopyItem) o;
            if (!(getSourceItem().equals(that.getSourceItem()) && getTargetItem().equals(that.getTargetItem()))) {
                return false;
            }
            return (this.getChunkData() == null && that.getChunkData() == null)
                    || (this.getChunkData() != null && that.getChunkData() != null
                            && this.getChunkData().getCurrentChunk().equals(that.getChunkData().getCurrentChunk()));
        }

        @Override
        public int hashCode() {
            return Objects.hash(getSourceItem(), getTargetItem(),
                    getChunkData() != null ? getChunkData().getCurrentChunk() : "");
        }

        public static class ChunkData {
            private final Long chunkSize;
            private final Integer currentChunk;

            public ChunkData(int currentChunk, Long chunkSize) {
                this.currentChunk = currentChunk;
                this.chunkSize = chunkSize;
            }

            public Long getChunkSize() {
                return chunkSize;
            }

            public Integer getCurrentChunk() {
                return currentChunk;
            }
        }
    }

    public static class IdCopyContext extends CopyContext {

        public IdCopyContext(String migrationId, MigrationContext migrationContext,
                PerformanceProfiler performanceProfiler) {
            super(migrationId, migrationContext, null, performanceProfiler);
        }

        @Override
        public Set<DataCopyItem> getCopyItems() {
            throw new UnsupportedOperationException("This is lean copy context without the actual copy items");
        }
    }
}
