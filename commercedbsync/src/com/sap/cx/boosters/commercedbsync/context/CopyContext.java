/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context;

import com.sap.cx.boosters.commercedbsync.performance.PerformanceProfiler;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;

/**
 * Contains the Information needed to Copy Data
 */
public class CopyContext {

    private String migrationId;
    private MigrationContext migrationContext;
    private Set<DataCopyItem> copyItems;
    private PerformanceProfiler performanceProfiler;

    public CopyContext(String migrationId, MigrationContext migrationContext, Set<DataCopyItem> copyItems, PerformanceProfiler performanceProfiler) {
        this.migrationId = migrationId;
        this.migrationContext = migrationContext;
        this.copyItems = copyItems;
        this.performanceProfiler = performanceProfiler;
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

    public static class DataCopyItem {
        private final String sourceItem;
        private final String targetItem;
        private final Map<String, String> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        private final Long rowCount;

        public DataCopyItem(String sourceItem, String targetItem) {
            this.sourceItem = sourceItem;
            this.targetItem = targetItem;
            this.rowCount = null;
        }

        public DataCopyItem(String sourceItem, String targetItem, Map<String, String> columnMap, Long rowCount) {
            this.sourceItem = sourceItem;
            this.targetItem = targetItem;
            this.columnMap.clear();
            this.columnMap.putAll(columnMap);
            this.rowCount = rowCount;
        }

        public String getSourceItem() {
            return sourceItem;
        }

        public String getTargetItem() {
            return targetItem;
        }

        public String getPipelineName() {
            return getSourceItem() + "->" + getTargetItem();
        }

        public Map<String, String> getColumnMap() {
            return columnMap;
        }

        public Long getRowCount() {
            return rowCount;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", DataCopyItem.class.getSimpleName() + "[", "]")
                    .add("sourceItem='" + sourceItem + "'")
                    .add("targetItem='" + targetItem + "'")
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataCopyItem that = (DataCopyItem) o;
            return getSourceItem().equals(that.getSourceItem()) &&
                    getTargetItem().equals(that.getTargetItem());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getSourceItem(), getTargetItem());
        }
    }

    public static class IdCopyContext extends CopyContext {

        public IdCopyContext(String migrationId, MigrationContext migrationContext, PerformanceProfiler performanceProfiler) {
            super(migrationId, migrationContext, null, performanceProfiler);
        }

        @Override
        public Set<DataCopyItem> getCopyItems() {
            throw new UnsupportedOperationException("This is lean copy context without the actual copy items");
        }
    }
}
