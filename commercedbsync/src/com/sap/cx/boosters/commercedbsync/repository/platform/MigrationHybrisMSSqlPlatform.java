/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatform;
import de.hybris.bootstrap.ddl.sql.HybrisMSSqlBuilder;
import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.mssql.MSSqlModelReader;
import org.apache.ddlutils.platform.mssql.MSSqlPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MigrationHybrisMSSqlPlatform extends MSSqlPlatform implements HybrisPlatform {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationHybrisMSSqlPlatform.class);


    private MigrationHybrisMSSqlPlatform() {
    }

    public static HybrisPlatform build(DatabaseSettings databaseSettings) {
        MigrationHybrisMSSqlPlatform instance = new MigrationHybrisMSSqlPlatform();
        instance.provideCustomMapping();
        instance.setSqlBuilder(new MigrationHybrisMSSqlBuilder(instance, databaseSettings));
        MigrationHybrisMSSqlPlatform.HybrisMSSqlModelReader reader = new MigrationHybrisMSSqlPlatform.HybrisMSSqlModelReader(instance);
        reader.setDefaultTablePattern(databaseSettings.getTablePrefix() + '%');
        instance.setModelReader(reader);
        return instance;
    }

    public Database readModelFromDatabase(String name) throws DatabaseOperationException {
        return this.readModelFromDatabase(name, (String) null, (String) null, (String[]) null);
    }

    private void provideCustomMapping() {
        PlatformInfo platformInfo = this.getPlatformInfo();
        platformInfo.setMaxColumnNameLength(30);
        platformInfo.addNativeTypeMapping(12002, "BIGINT", -5);
        platformInfo.addNativeTypeMapping(12000, "NVARCHAR(MAX)", -1);
        platformInfo.addNativeTypeMapping(12003, "NVARCHAR(MAX)", -1);
        platformInfo.addNativeTypeMapping(12001, "NVARCHAR(MAX)", -1);
        platformInfo.addNativeTypeMapping(-5, "BIGINT");
        platformInfo.addNativeTypeMapping(12, "NVARCHAR");
        platformInfo.addNativeTypeMapping(-7, "TINYINT");
        platformInfo.addNativeTypeMapping(4, "INTEGER");
        platformInfo.addNativeTypeMapping(5, "INTEGER");
        platformInfo.addNativeTypeMapping(-6, "TINYINT", -6);
        platformInfo.addNativeTypeMapping(8, "FLOAT", 8);
        platformInfo.addNativeTypeMapping(6, "FLOAT", 8);
        platformInfo.addNativeTypeMapping(-9, "NVARCHAR", -9);
        platformInfo.addNativeTypeMapping(92, "DATETIME2", 93);
        platformInfo.addNativeTypeMapping(93, "DATETIME2");
        platformInfo.addNativeTypeMapping(2004, "VARBINARY(MAX)");
    }

    public String getTableName(Table table) {
        return this.getSqlBuilder().getTableName(table);
    }

    public String getColumnName(Column column) {
        return ((HybrisMSSqlBuilder) this.getSqlBuilder()).getColumnName(column);
    }

    @Override
    public void alterTables(Connection connection, Database desiredModel, boolean continueOnError) throws DatabaseOperationException {
        String sql = this.getAlterTablesSql(connection, desiredModel);
        LOG.info(sql);
        this.evaluateBatch(connection, sql, continueOnError);
    }

    private static class HybrisMSSqlModelReader extends MSSqlModelReader {
        private static final String TABLE_NAME_KEY = "TABLE_NAME";
        private final Set<String> tablesToExclude = new HashSet<String>() {
            {
                this.add("trace_xe_action_map");
                this.add("trace_xe_event_map");
            }
        };

        public HybrisMSSqlModelReader(Platform platform) {
            super(platform);
        }

        protected Table readTable(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
            return this.tableShouldBeExcluded(values) ? null : super.readTable(metaData, values);
        }

        private boolean tableShouldBeExcluded(Map values) {
            String tableName = this.getTableNameFrom(values);
            return tableName != null && this.tablesToExclude.contains(tableName.toLowerCase());
        }

        private String getTableNameFrom(Map values) {
            return (String) values.get("TABLE_NAME");
        }
    }
}
