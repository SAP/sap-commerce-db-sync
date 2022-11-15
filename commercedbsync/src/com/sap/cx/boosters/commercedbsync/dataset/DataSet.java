/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.dataset;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.sap.cx.boosters.commercedbsync.dataset.impl.DefaultDataSet;

import java.util.Collections;
import java.util.List;

public interface DataSet {

    DataSet EMPTY = new DefaultDataSet(0, Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    int getColumnCount();

    List<List<Object>> getAllResults();

    Object getColumnValue(String column, List<Object> row);

    Object getColumnValueForPostGres(String columnName, List<Object> row, DataColumn sourceColumnType, int targetColumnType);

    boolean isNotEmpty();

    boolean hasColumn(String column);

    ISQLServerBulkData toSQLServerBulkData();
}
