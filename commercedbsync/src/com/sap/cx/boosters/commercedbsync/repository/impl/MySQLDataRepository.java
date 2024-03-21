/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.repository.platform.MigrationHybrisMySqlPlatform;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;

import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatform;
import org.apache.ddlutils.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class MySQLDataRepository extends AbstractDataRepository {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLDataRepository.class);

    public MySQLDataRepository(MigrationContext migrationContext, DataSourceConfiguration dataSourceConfiguration,
            DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(migrationContext, dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected Platform createPlatform(DatabaseSettings databaseSettings, DataSource dataSource) {
        final HybrisPlatform platform = MigrationHybrisMySqlPlatform.build(databaseSettings);
        platform.setDataSource(dataSource);
        return platform;
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        return String.format("select * from %s where %s order by %s limit ?,?", queryDefinition.getTable(),
                expandConditions(conditions), queryDefinition.getOrderByColumns());
    }

    @Override
    public void runSqlScript(Resource resource) {
        final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.setIgnoreFailedDrops(true);
        databasePopulator.setSeparator("#");
        databasePopulator.execute(getDataSource());
    }

    @Override
    protected boolean hasParameterizedOffsetBatchQuery() {
        return true;
    }

    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        return String.format("select * from %s where %s order by %s limit %s", queryDefinition.getTable(),
                expandConditions(conditions), queryDefinition.getColumn(), queryDefinition.getBatchSize());
    }

    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        String column = queryDefinition.getColumn();
        // spotless:off
        return String.format(
                "SELECT %s,rownum\n" +
                "FROM ( \n" +
                "    SELECT \n" +
                "        @row := @row +1 AS rownum, %s \n" +
                "    FROM (SELECT @row :=-1) r, %s  WHERE %s ORDER BY %s) ranked \n" +
                "WHERE rownum %% ? = 0",
        // spotless:on
                column, column, queryDefinition.getTable(), expandConditions(conditions), column);
    }

    @Override
    protected boolean hasParameterizedBatchMarkersQuery() {
        return true;
    }

    @Override
    protected String createAllTableNamesQuery() {
        return String.format(
                "select TABLE_NAME from information_schema.tables where table_schema = '%s' and TABLE_TYPE = 'BASE TABLE'",
                getDataSourceConfiguration().getSchema());
    }

    @Override
    protected String createAllColumnNamesQuery(String tableName) {
        return String.format(
                "SELECT DISTINCT COLUMN_NAME from information_schema.columns where table_schema = '%s' AND TABLE_NAME = '%s'",
                getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        // spotless:off
        return String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS t1\n" +
                        "INNER JOIN \n" +
                        "(\n" +
                        "SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, count(INDEX_NAME) as COL_COUNT \n" +
                        "FROM INFORMATION_SCHEMA.STATISTICS \n" +
                        "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND NON_UNIQUE = 0\n" +
                        "GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME\n" +
                        "ORDER BY COL_COUNT ASC\n" +
                        "LIMIT 1\n" +
                        ") t2\n" +
                        "ON t1.TABLE_SCHEMA = t2.TABLE_SCHEMA AND t1.TABLE_NAME = t2.TABLE_NAME AND t1.INDEX_NAME = t2.INDEX_NAME\n" +
                        ";\n",
        // spotless:on
                getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    protected String getBulkInsertStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues) {
        return null;
    }

    @Override
    protected String getBulkUpdateStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues,
            List<String> upsertIDs) {
        return null;
    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.MYSQL;
    }

    @Override
    public String buildBulkUpsertStatement(String table, List<String> columnsToCopy, List<String> upsertIDs) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public String getDatabaseTimezone() {
        String query = "SELECT @@system_time_zone as timezone";
        try (Connection conn = super.getConnection(); PreparedStatement stmt = conn.prepareStatement(query)) {
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getString("timezone");
            }
        } catch (Exception e) {
            LOG.warn("Failed to check database timezone", e);
        }
        return null;
    }
}
