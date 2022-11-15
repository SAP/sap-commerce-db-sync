/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.dataset.impl;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import org.apache.logging.log4j.util.Strings;
import com.sap.cx.boosters.commercedbsync.dataset.DataColumn;

import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BulkDataSet extends DefaultDataSet implements ISQLServerBulkData {

    private final Map<Integer, DataColumn> typeMap = new HashMap<>();
    private int pointer = -1;
    private Set<Integer> columnOrdinals;

    public BulkDataSet(int columnCount, List<DataColumn> columnOrder, List<List<Object>> result) {
        super(columnCount, columnOrder, result);
        this.columnOrdinals = IntStream.range(1, columnOrder.size() + 1).boxed().collect(Collectors.toSet());
        this.typeMap.put(Types.BLOB, new DefaultDataColumn(Strings.EMPTY, Types.LONGVARBINARY, 0x7FFFFFFF, 0));
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        return columnOrdinals;
    }

    @Override
    public String getColumnName(int i) {
        return getColumnOrder().get(i - 1).getColumnName();
    }

    @Override
    public int getColumnType(int i) {
        return mapColumn(getColumnOrder().get(i - 1)).getColumnType();
    }

    @Override
    public int getPrecision(int i) {
        return mapColumn(getColumnOrder().get(i - 1)).getPrecision();
    }

    @Override
    public int getScale(int i) {
        return mapColumn(getColumnOrder().get(i - 1)).getScale();
    }

    @Override
    public Object[] getRowData() throws SQLException {
        return getAllResults().get(pointer).toArray();
    }

    @Override
    public boolean next() throws SQLException {
        pointer++;
        return getAllResults().size() > pointer;
    }

    private DataColumn mapColumn(DataColumn column) {
        if (typeMap.containsKey(column.getColumnType())) {
            return typeMap.get(column.getColumnType());
        }
        return column;
    }

}
