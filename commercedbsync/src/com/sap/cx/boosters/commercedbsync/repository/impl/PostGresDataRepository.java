/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import javax.sql.DataSource;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import org.apache.ddlutils.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.repository.platform.MigrationHybrisPostGresPlatform;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;

import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatform;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PostGresDataRepository extends AbstractDataRepository {
    private static final Logger LOG = LoggerFactory.getLogger(PostGresDataRepository.class);

    public PostGresDataRepository(MigrationContext migrationContext, DataSourceConfiguration dataSourceConfiguration,
            DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(migrationContext, dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        final String batchQuery = String.format(
                "SELECT * FROM %s WHERE %s ORDER BY %s OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getOrderByColumns());
        return batchQuery;
    }

    @Override
    protected boolean hasParameterizedOffsetBatchQuery() {
        return true;
    }

    @Override
    public void runSqlScript(Resource resource) {
        final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.setIgnoreFailedDrops(true);
        databasePopulator.setSeparator("#");
        databasePopulator.execute(getDataSource());
    }

    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        return String.format("select * from %s where %s order by %s limit %s", queryDefinition.getTable(),
                expandConditions(conditions), queryDefinition.getColumn(), queryDefinition.getBatchSize());
    }

    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        final String column = queryDefinition.getColumn();
        final String tableName = queryDefinition.getTable();
        // spotless:off
        return String.format("SELECT t.%s, t.rownum\n" +
                        "FROM\n" +
                        "(\n" +
                        "    SELECT %s, (ROW_NUMBER() OVER (ORDER BY %s))-1 AS rownum\n" +
                        "    FROM %s\n WHERE %s" +
                        ") AS t\n" +
                        "WHERE t.rownum %% ? = 0\n" +
                        "ORDER BY t.%s",
                // spotless:on
                column, column, column, tableName, expandConditions(conditions), column);
    }

    @Override
    protected boolean hasParameterizedBatchMarkersQuery() {
        return true;
    }

    @Override
    protected String getLastValueCondition() {
        /*
         * In case of PostgreSQL JDBC driver was complaining about batch marker being a
         * varchar: `org.postgresql.util.PSQLException: ERROR: operator does not exist:
         * bigint >= character varying`
         */
        return "%s >= CAST(? AS BIGINT)";
    }

    @Override
    protected String getNextValueCondition() {
        return "%s < CAST(? AS BIGINT)";
    }

    @Override
    protected String createAllTableNamesQuery() {
        return String.format(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE = 'BASE TABLE'",
                getDataSourceConfiguration().getSchema());
    }

    @Override
    protected String createAllColumnNamesQuery(String tableName) {
        return String.format(
                "SELECT DISTINCT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        // spotless:off
        return String.format(
                "SELECT COLUMN_NAME\n" +
                        "   FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C\n" +
                        "      JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CC\n" +
                        "         USING (TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME)\n" +
                        "   WHERE C.CONSTRAINT_TYPE IN ('UNIQUE', 'PRIMARY KEY')\n" +
                        "     AND TABLE_SCHEMA = '%s'\n" +
                        "     AND TABLE_NAME = '%s';",
                // spotless:on
                getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    protected void addCustomPlatformTypeMapping(final Platform platform) {

        // DO nothing
    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.POSTGRESQL;
    }

    @Override
    public String buildBulkUpsertStatement(String table, List<String> columnsToCopy, List<String> upsertIDs) {
        // example:
        // https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/
        final StringBuilder sqlBuilder = new StringBuilder();

        sqlBuilder.append("INSERT INTO ").append(table).append(" \n");
        sqlBuilder
                .append(getBulkInsertStatementParamList(columnsToCopy, Collections.nCopies(columnsToCopy.size(), "?")));
        sqlBuilder.append(String.format(" ON CONFLICT (%s) DO UPDATE ", upsertIDs.get(0))).append('\n');
        sqlBuilder.append(getBulkUpdateStatementParamList(columnsToCopy, Collections.emptyList(), upsertIDs));

        return sqlBuilder.toString();
    }

    @Override
    protected String getBulkInsertStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues) {
        return "(" + String.join(", ", columnsToCopy) + ") VALUES (" + String.join(", ", columnsToCopyValues) + ")";
    }

    @Override
    protected String getBulkUpdateStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues,
            List<String> upsertIDs) {
        return "SET " + columnsToCopy.stream().filter(s -> !s.equalsIgnoreCase(upsertIDs.get(0)))
                .map(column -> String.format("%s = EXCLUDED.%s", column, column)).collect(Collectors.joining(", "));
    }

    @Override
    protected Platform createPlatform(DatabaseSettings databaseSettings, DataSource dataSource) {
        HybrisPlatform instance = MigrationHybrisPostGresPlatform.build(databaseSettings);
        instance.setDataSource(dataSource);
        return instance;
    }

    @Override
    public String getDatabaseTimezone() {
        String query = "SELECT abbrev FROM pg_timezone_names WHERE name = current_setting('TIMEZONE')";
        try (Connection conn = super.getConnection(); PreparedStatement stmt = conn.prepareStatement(query)) {
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getString("abbrev");
            }
        } catch (Exception e) {
            LOG.warn("Failed to check database timezone", e);
        }
        return null;
    }
}
