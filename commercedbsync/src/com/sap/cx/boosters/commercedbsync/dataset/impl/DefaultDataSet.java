/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.dataset.impl;

import com.github.freva.asciitable.AsciiTable;
import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import com.sap.cx.boosters.commercedbsync.dataset.DataColumn;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Immutable
public class DefaultDataSet implements DataSet {

    private final int columnCount;
    private final List<DataColumn> columnOrder;
    private final List<List<Object>> result;

    public DefaultDataSet(int columnCount, List<DataColumn> columnOrder, List<List<Object>> result) {
        this.columnCount = columnCount;
        // TODO REVIEW Downgraded from Java8 to Java11
        this.columnOrder = Collections.unmodifiableList(columnOrder);
        this.result = Collections.unmodifiableList(result.stream().map(Collections::unmodifiableList).collect(Collectors.toList()));
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public List<List<Object>> getAllResults() {
        return result;
    }

    @Override
    public Object getColumnValue(String columnName, List<Object> row) {
        if (columnName == null || !hasColumn(columnName)) {
            throw new IllegalArgumentException(String.format("Column %s is not part of the result", columnName));
        }
        int idx = IntStream.range(0, columnOrder.size()).filter(i -> columnName.equalsIgnoreCase(columnOrder.get(i).getColumnName())).findFirst().getAsInt();
        return row.get(idx);
    }

    @Override
    public Object getColumnValueForPostGres(String columnName, List<Object> row, DataColumn sourceColumnType, int targetColumnType) {
        if (columnName == null || !hasColumn(columnName)) {
            throw new IllegalArgumentException(String.format("Column %s is not part of the result", columnName));
        }
        int idx = IntStream.range(0, columnOrder.size()).filter(i -> columnName.equalsIgnoreCase(columnOrder.get(i).getColumnName())).findFirst().getAsInt();
        Object  columnValue = row.get(idx);
        if(ObjectUtils.isNotEmpty(columnValue)){
            switch (sourceColumnType.getColumnType()) {
                case 1:
                    if (sourceColumnType.getPrecision() == 4 && targetColumnType == 5) {
                        if (columnValue instanceof String && ((String) columnValue).trim().length() == 1) {
                            columnValue = (int) (((String) columnValue).trim().charAt(0));
                        }
                    }
                    break;
                default:
                    break;

            }
        }
        return columnValue;
    }

    public Object getColumnValueForHANA(String columnName, List<Object> row, DataColumn sourceColumnType, int targetColumnType) {
        if (columnName == null || !hasColumn(columnName)) {
            throw new IllegalArgumentException(String.format("Column %s is not part of the result", columnName));
        }
        int idx = IntStream.range(0, columnOrder.size()).filter(i -> columnName.equalsIgnoreCase(columnOrder.get(i).getColumnName())).findFirst().getAsInt();
        Object  columnValue = row.get(idx);
        if(ObjectUtils.isNotEmpty(columnValue)){
            switch (sourceColumnType.getColumnType()) {
                case 1:
                    if (sourceColumnType.getPrecision() == 4 && targetColumnType == 5) {
                        if (columnValue instanceof String && ((String) columnValue).trim().length() == 1) {
                            columnValue = (int) (((String) columnValue).trim().charAt(0));
                        }
                    }
                    break;

                default:
                    break;
            }
        }
        return columnValue;
    }

    @Override
    public boolean isNotEmpty() {
        return getAllResults() != null && getAllResults().size() > 0;
    }

    @Override
    public boolean hasColumn(String column) {
        if (StringUtils.isEmpty(column)) {
            return false;
        }
        return columnOrder.stream().map(DataColumn::getColumnName).anyMatch(column::equalsIgnoreCase);
    }

    public String toString() {
        String[] headers = columnOrder.stream().map(DataColumn::getColumnName).toArray(String[]::new);
        String[][] data = getAllResults().stream()
                .map(l -> l.stream().map(v -> String.valueOf(v)).toArray(String[]::new))
                .toArray(String[][]::new);
        return AsciiTable.getTable(headers, data);
    }

    public List<DataColumn> getColumnOrder() {
        return columnOrder;
    }

    @Override
    public ISQLServerBulkData toSQLServerBulkData() {
        return new BulkDataSet(columnCount, columnOrder, result);
    }
}
