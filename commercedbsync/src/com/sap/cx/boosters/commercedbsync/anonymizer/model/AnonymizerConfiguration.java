/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer.model;

import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnonymizerConfiguration {
    private List<Table> tables;
    private final Map<String, Table> tableMap = new HashMap<>();

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        CollectionUtils.emptyIfNull(tables).forEach(table -> tableMap.put(table.getName(), table));
        this.tables = tables;
    }

    public Table getTable(String tableName) {
        return tableMap.get(tableName);
    }
}
