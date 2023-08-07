/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.dataset.impl;

import com.github.freva.asciitable.AsciiTable;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import com.sap.cx.boosters.commercedbsync.dataset.DataColumn;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

import static java.sql.Types.CHAR;
import static java.sql.Types.SMALLINT;

@Immutable
public class DefaultDataSet implements DataSet {

    private final int batchId;
    private final int columnCount;
    private final List<DataColumn> columnOrder;
    private final List<List<Object>> result;

    public DefaultDataSet(int batchId, int columnCount, List<DataColumn> columnOrder, List<List<Object>> result) {
        this.batchId = batchId;
        this.columnCount = columnCount;
        this.columnOrder = Collections.unmodifiableList(columnOrder);
        this.result = result.stream().map(Collections::unmodifiableList).toList();
    }

    @Override
    public int getBatchId() {
        return batchId;
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
    public Object getColumnValue(String columnName, List<Object> row, DataColumn sourceColumn, int targetColumnType) {
        if (columnName == null || !hasColumn(columnName)) {
            throw new IllegalArgumentException(String.format("Column %s is not part of the result", columnName));
        }

        Object columnValue = row.get(findColumnIndex(columnName));

        if (ObjectUtils.isNotEmpty(columnValue)) {
            switch (sourceColumn.getColumnType()) {
                case CHAR :
                    if (sourceColumn.getPrecision() == 4 && targetColumnType == SMALLINT) {
                        if (columnValue instanceof String && ((String) columnValue).length() == 4) {
                            columnValue = (int) (((String) columnValue).charAt(0)); // cannot use trim() to not loose
                                                                                    // \n, \t, space etc chars
                        }
                    }
                    break;
                case SMALLINT :
                    if (targetColumnType == CHAR && StringUtils.isNumeric(String.valueOf(columnValue))) {
                        columnValue = Character.toString((char) ((Integer) columnValue).intValue());
                    }
                    break;
                default :
                    break;

            }
        }

        return columnValue;
    }

    @Override
    public boolean isNotEmpty() {
        return getAllResults() != null && !getAllResults().isEmpty();
    }

    @Override
    public boolean hasColumn(String column) {
        if (StringUtils.isEmpty(column)) {
            return false;
        }
        return columnOrder.stream().map(DataColumn::getColumnName).anyMatch(column::equalsIgnoreCase);
    }

    @Override
    public DataColumn getColumn(int columnIndex) {
        return IterableUtils.get(columnOrder, columnIndex);
    }

    @Override
    public DataColumn getColumn(String columnName) {
        return IterableUtils.get(columnOrder, findColumnIndex(columnName));
    }

    protected int findColumnIndex(String columnName) {
        return IterableUtils.indexOf(columnOrder,
                dataColumn -> dataColumn.getColumnName().equalsIgnoreCase(columnName));
    }

    public String toString() {
        String[] headers = columnOrder.stream().map(DataColumn::getColumnName).toArray(String[]::new);
        String[][] data = getAllResults().stream().map(l -> l.stream().map(String::valueOf).toArray(String[]::new))
                .toArray(String[][]::new);
        return AsciiTable.getTable(headers, data);
    }
}
