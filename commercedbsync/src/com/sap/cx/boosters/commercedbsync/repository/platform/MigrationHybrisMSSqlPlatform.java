/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatform;
import de.hybris.bootstrap.ddl.sql.HybrisMSSqlBuilder;
import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.JdbcModelReader;
import org.apache.ddlutils.platform.mssql.MSSqlModelReader;
import org.apache.ddlutils.platform.mssql.MSSqlPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MigrationHybrisMSSqlPlatform extends MSSqlPlatform implements HybrisPlatform {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationHybrisMSSqlPlatform.class);

    private static final String[] KNOWN_SYSTEM_TABLES = new String[]{"dtproperties"};

    private MigrationHybrisMSSqlPlatform() {
    }

    public static HybrisPlatform build(DatabaseSettings databaseSettings) {
        MigrationHybrisMSSqlPlatform instance = new MigrationHybrisMSSqlPlatform();
        instance.provideCustomMapping();
        instance.setSqlBuilder(new MigrationHybrisMSSqlBuilder(instance, databaseSettings));
        MigrationHybrisMSSqlPlatform.HybrisMSSqlModelReader reader = new MigrationHybrisMSSqlPlatform.HybrisMSSqlModelReader(
                instance);
        reader.setDefaultTablePattern(databaseSettings.getTablePrefix() + '%');
        instance.setModelReader(reader);
        return instance;
    }

    public Database readModelFromDatabase(String name) throws DatabaseOperationException {
        return this.readModelFromDatabase(name, null, null, null);
    }

    private void provideCustomMapping() {
        PlatformInfo platformInfo = this.getPlatformInfo();
        platformInfo.setMaxColumnNameLength(30);
        platformInfo.addNativeTypeMapping(12002, "BIGINT", Types.BIGINT);
        platformInfo.addNativeTypeMapping(12000, "NVARCHAR(MAX)", Types.LONGVARCHAR);
        platformInfo.addNativeTypeMapping(12003, "NVARCHAR(MAX)", Types.LONGVARCHAR);
        platformInfo.addNativeTypeMapping(12001, "NVARCHAR(MAX)", Types.LONGVARCHAR);
        platformInfo.addNativeTypeMapping(Types.BIGINT, "BIGINT");
        platformInfo.addNativeTypeMapping(Types.VARCHAR, "NVARCHAR");
        platformInfo.addNativeTypeMapping(Types.BIT, "TINYINT");
        platformInfo.addNativeTypeMapping(Types.INTEGER, "INTEGER");
        platformInfo.addNativeTypeMapping(Types.SMALLINT, "INTEGER");
        platformInfo.addNativeTypeMapping(Types.TINYINT, "TINYINT", Types.TINYINT);
        platformInfo.addNativeTypeMapping(Types.DOUBLE, "FLOAT", Types.DOUBLE);
        platformInfo.addNativeTypeMapping(Types.FLOAT, "FLOAT", Types.DOUBLE);
        platformInfo.addNativeTypeMapping(Types.NVARCHAR, "NVARCHAR", Types.NVARCHAR);
        platformInfo.addNativeTypeMapping(Types.TIME, "DATETIME2", Types.TIMESTAMP);
        platformInfo.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME2");
        platformInfo.addNativeTypeMapping(Types.BLOB, "VARBINARY(MAX)");
    }

    public String getTableName(Table table) {
        return this.getSqlBuilder().getTableName(table);
    }

    public String getColumnName(Column column) {
        return ((HybrisMSSqlBuilder) this.getSqlBuilder()).getColumnName(column);
    }

    @Override
    public void alterTables(Connection connection, Database desiredModel, boolean continueOnError)
            throws DatabaseOperationException {
        String sql = this.getAlterTablesSql(connection, desiredModel);
        LOG.info(sql);
        this.evaluateBatch(connection, sql, continueOnError);
    }

    private static class HybrisMSSqlModelReader extends MSSqlModelReader {
        private static final String TABLE_NAME_KEY = "TABLE_NAME";

        private final Set<String> tablesToExclude = Set.of("trace_xe_action_map", "trace_xe_event_map");

        public HybrisMSSqlModelReader(Platform platform) {
            super(platform);
        }

        protected Table readTable(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
            if (this.tableShouldBeExcluded(values)) {
                return null;
            } else {
                String tableName = (String)values.get("TABLE_NAME");

                for(int idx = 0; idx < KNOWN_SYSTEM_TABLES.length; ++idx) {
                    if (KNOWN_SYSTEM_TABLES[idx].equals(tableName)) {
                        return null;
                    }
                }

                Table table = superReadTable(metaData, values);
                if (table != null) {
                    determineAutoIncrementFromResultSetMetaData(table, table.getColumns());
                    int idx = 0;

                    while(true) {
                        while(idx < table.getIndexCount()) {
                            Index index = table.getIndex(idx);
                            if (index.isUnique() && this.existsPKWithName(metaData, table, index.getName())) {
                                table.removeIndex(idx);
                            } else {
                                ++idx;
                            }
                        }

                        return table;
                    }
                } else {
                    return table;
                }
            }
        }

        private Table superReadTable(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
            String tableName = (String)values.get("TABLE_NAME");
            Table table = null;
            if (tableName != null && tableName.length() > 0) {
                table = new Table();
                table.setName(tableName);
                table.setType((String)values.get("TABLE_TYPE"));
                table.setCatalog((String)values.get("TABLE_CAT"));
                table.setSchema((String)values.get("TABLE_SCHEM"));
                table.setDescription((String)values.get("REMARKS"));
                table.addColumns(this.readColumns(metaData, tableName));
                table.addForeignKeys(this.readForeignKeys(metaData, tableName));
                table.addIndices(this.readIndices(metaData, tableName));
                Collection primaryKeys = this.readPrimaryKeyNames(metaData, tableName);
                Iterator it = primaryKeys.iterator();

                while(it.hasNext()) {
                    table.findColumn((String)it.next(), true).setPrimaryKey(true);
                }

                if (this.getPlatformInfo().isSystemIndicesReturned()) {
                    this.removeSystemIndices(metaData, table);
                }
            }

            return table;
        }

        private boolean existsPKWithName(DatabaseMetaDataWrapper metaData, Table table, String name) {
            try {
                ResultSet pks = metaData.getPrimaryKeys(table.getName());
                boolean found = false;

                while(pks.next() && !found) {
                    if (name.equals(pks.getString("PK_NAME"))) {
                        found = true;
                    }
                }

                pks.close();
                return found;
            } catch (SQLException var6) {
                throw new DdlUtilsException(var6);
            }
        }

        protected void determineAutoIncrementFromResultSetMetaData(Table table, Column[] columnsToCheck) throws SQLException {
            if (columnsToCheck != null && columnsToCheck.length != 0) {
                StringBuffer query = new StringBuffer();
                query.append("SELECT ");

                for(int idx = 0; idx < columnsToCheck.length; ++idx) {
                    if (idx > 0) {
                        query.append(",");
                    }

                    if (this.getPlatform().isDelimitedIdentifierModeOn()) {
                        query.append(this.getPlatformInfo().getDelimiterToken());
                    }

                    query.append(this.getPlatformInfo().getValueQuoteToken());
                    query.append(columnsToCheck[idx].getName());
                    query.append(this.getPlatformInfo().getValueQuoteToken());
                    if (this.getPlatform().isDelimitedIdentifierModeOn()) {
                        query.append(this.getPlatformInfo().getDelimiterToken());
                    }
                }

                query.append(" FROM ");
                if (this.getPlatform().isDelimitedIdentifierModeOn()) {
                    query.append(this.getPlatformInfo().getDelimiterToken());
                }

                query.append(table.getName());
                if (this.getPlatform().isDelimitedIdentifierModeOn()) {
                    query.append(this.getPlatformInfo().getDelimiterToken());
                }

                query.append(" WHERE 1 = 0");
                Statement stmt = null;

                try {
                    stmt = this.getConnection().createStatement();
                    ResultSet rs = stmt.executeQuery(query.toString());
                    ResultSetMetaData rsMetaData = rs.getMetaData();

                    for(int idx = 0; idx < columnsToCheck.length; ++idx) {
                        if (rsMetaData.isAutoIncrement(idx + 1)) {
                            columnsToCheck[idx].setAutoIncrement(true);
                        }
                    }
                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }

                }

            }
        }

        private boolean tableShouldBeExcluded(Map values) {
            String tableName = this.getTableNameFrom(values);
            return tableName != null && this.tablesToExclude.contains(tableName.toLowerCase());
        }

        private String getTableNameFrom(Map values) {
            return (String) values.get(TABLE_NAME_KEY);
        }
    }
}
