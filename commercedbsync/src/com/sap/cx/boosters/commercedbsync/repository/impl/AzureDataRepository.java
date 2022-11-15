/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import com.google.common.base.Joiner;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatform;
import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.Platform;
import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import com.sap.cx.boosters.commercedbsync.repository.platform.MigrationHybrisMSSqlPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class AzureDataRepository extends AbstractDataRepository {

    private static final Logger LOG = LoggerFactory.getLogger(AzureDataRepository.class);

    private static final String LP_SUFFIX = "lp";

    public AzureDataRepository(DataSourceConfiguration dataSourceConfiguration, DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected void addCustomPlatformTypeMapping(Platform platform) {
        platform.getPlatformInfo().addNativeTypeMapping(Types.NCLOB, "NVARCHAR(MAX)");
        platform.getPlatformInfo().addNativeTypeMapping(Types.CLOB, "NVARCHAR(MAX)");
        platform.getPlatformInfo().addNativeTypeMapping(Types.LONGVARCHAR, "NVARCHAR(MAX)");
        platform.getPlatformInfo().addNativeTypeMapping(Types.VARBINARY, "VARBINARY");
        platform.getPlatformInfo().addNativeTypeMapping(Types.REAL, "float");
        platform.getPlatformInfo().addNativeTypeMapping(Types.LONGVARBINARY, "VARBINARY(MAX)");
        platform.getPlatformInfo().addNativeTypeMapping(Types.NCHAR, "NVARCHAR");
        platform.getPlatformInfo().setHasSize(Types.NCHAR, true);
        platform.getPlatformInfo().setHasSize(Types.VARBINARY, true);
        platform.getPlatformInfo().setHasSize(Types.NVARCHAR, true);
        platform.getPlatformInfo().setHasPrecisionAndScale(Types.REAL, false);
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        String orderBy = Joiner.on(',').join(queryDefinition.getAllColumns());
        return String.format("SELECT * FROM %s WHERE %s ORDER BY %s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY", queryDefinition.getTable(), expandConditions(conditions), orderBy, queryDefinition.getOffset(), queryDefinition.getBatchSize());
    }

    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        return String.format("select top %s * from %s where %s order by %s", queryDefinition.getBatchSize(), queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getColumn());
    }

    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        String column = queryDefinition.getColumn();
        String tableName = queryDefinition.getTable();
        if (queryDefinition.isLpTableEnabled()){
            tableName = getLpTableName(tableName);
        }
        return String.format("SELECT t.%s, t.rownum\n" +
                "FROM\n" +
                "(\n" +
                "    SELECT %s, (ROW_NUMBER() OVER (ORDER BY %s))-1 AS rownum\n" +
                "    FROM %s\n WHERE %s" +
                ") AS t\n" +
                "WHERE t.rownum %% %s = 0\n" +
                "ORDER BY t.%s", column, column, column, tableName, expandConditions(conditions), queryDefinition.getBatchSize(), column);
    }

    @Override
    protected String createAllTableNamesQuery() {
        return String.format(
                "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s'",
                getDataSourceConfiguration().getSchema());
    }

    @Override
    protected String createAllColumnNamesQuery(String tableName) {
        return String.format(
                "SELECT DISTINCT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    protected String getDisableIndexesScript(String table) {
        return String.format("SELECT 'ALTER INDEX ' + QUOTENAME(I.name) + ' ON ' +  QUOTENAME(SCHEMA_NAME(T.schema_id))+'.'+ QUOTENAME(T.name) + ' DISABLE' FROM sys.indexes I INNER JOIN sys.tables T ON I.object_id = T.object_id WHERE I.type_desc = 'NONCLUSTERED' AND I.name IS NOT NULL AND I.is_disabled = 0 AND T.name = '%s'", table);
    }

    @Override
    protected String getEnableIndexesScript(String table) {
        return String.format("SELECT 'ALTER INDEX ' + QUOTENAME(I.name) + ' ON ' +  QUOTENAME(SCHEMA_NAME(T.schema_id))+'.'+ QUOTENAME(T.name) + ' REBUILD' FROM sys.indexes I INNER JOIN sys.tables T ON I.object_id = T.object_id WHERE I.type_desc = 'NONCLUSTERED' AND I.name IS NOT NULL AND I.is_disabled = 1 AND T.name = '%s'", table);
    }

    @Override
    protected String getDropIndexesScript(String table) {
        return String.format("SELECT 'DROP INDEX ' + QUOTENAME(I.name) + ' ON ' +  QUOTENAME(SCHEMA_NAME(T.schema_id))+'.'+ QUOTENAME(T.name) FROM sys.indexes I INNER JOIN sys.tables T ON I.object_id = T.object_id WHERE I.type_desc = 'NONCLUSTERED' AND I.name IS NOT NULL AND T.name = '%s'", table);
    }

    @Override
    public float getDatabaseUtilization() throws SQLException {
        String query = "SELECT TOP 1 end_time, (SELECT Max(v) FROM (VALUES (avg_cpu_percent),(avg_data_io_percent),(avg_log_write_percent)) AS value(v)) AS [avg_DTU_percent] FROM sys.dm_db_resource_stats ORDER by end_time DESC;";
        try (Connection connection = getConnection();
             Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(query);
        ) {
            if (resultSet.next()) {
                return resultSet.getFloat("avg_DTU_percent");
            } else {
                //LOG.debug("There are no data with regard to Azure DTU");
                return -1;
            }
        } catch (Exception e) {
            LOG.trace("could not load database utilization stats");
            return -1;
        }
    }

    @Override
    protected Platform createPlatform(DatabaseSettings databaseSettings, DataSource dataSource) {
        HybrisPlatform instance = MigrationHybrisMSSqlPlatform.build(databaseSettings);
        instance.setDataSource(dataSource);
        return instance;
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        return String.format("SELECT col.name FROM (\n" +
                "SELECT TOP (1)\n" +
                "     SchemaName = t.schema_id,\n" +
                "     ObjectId = ind.object_id,\n" +
                "     IndexId = ind.index_id,\n" +
                "     TableName = t.name,\n" +
                "     IndexName = ind.name,\n" +
                "     ColCount = count(*)\n" +
                "FROM \n" +
                "     sys.indexes ind \n" +
                "INNER JOIN \n" +
                "     sys.tables t ON ind.object_id = t.object_id \n" +
                "WHERE \n" +
                "     t.name = '%s'\n" +
                "     AND\n" +
                "     SCHEMA_NAME(t.schema_id) = '%s'\n" +
                "     AND\n" +
                "     ind.is_unique = 1\n" +
                "GROUP BY t.schema_id,ind.object_id,ind.index_id,t.name,ind.name\n" +
                "ORDER BY ColCount ASC\n" +
                ") t1\n" +
                "INNER JOIN \n" +
                "     sys.index_columns ic ON  t1.ObjectId = ic.object_id and t1.IndexId = ic.index_id \n" +
                "INNER JOIN \n" +
                "     sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id ", tableName, getDataSourceConfiguration().getSchema());
    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.MSSQL;
    }

    private String getLpTableName(String tableName){
        return StringUtils.removeEndIgnoreCase(tableName,LP_SUFFIX);
    }
}
