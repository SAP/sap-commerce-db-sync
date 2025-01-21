/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.dataset;

import com.sap.cx.boosters.commercedbsync.dataset.impl.DefaultDataSet;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public interface DataSet {

    DataSet EMPTY = new DefaultDataSet(0, 0, Collections.emptyList(), Collections.emptyList(), null);

    int getBatchId();

    int getColumnCount();

    List<List<Object>> getAllResults();

    Object getColumnValue(String column, List<Object> row, DataColumn sourceColumn, int targetColumnType);

    default Object getColumnValue(String column, List<Object> row) {
        var dataColumn = getColumn(column);

        Objects.requireNonNull(dataColumn);

        return getColumnValue(column, row, dataColumn, dataColumn.getColumnType());
    }

    boolean isNotEmpty();

    boolean hasColumn(String column);

    DataColumn getColumn(int columnIndex);

    DataColumn getColumn(String columnName);

    String getPartition();
}
