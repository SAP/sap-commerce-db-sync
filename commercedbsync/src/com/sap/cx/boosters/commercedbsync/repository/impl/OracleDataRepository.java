/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import com.google.common.base.Joiner;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import org.apache.ddlutils.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;

import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisOraclePlatform;
import de.hybris.bootstrap.ddl.HybrisPlatform;

public class OracleDataRepository extends AbstractDataRepository {
    private static final Logger LOG = LoggerFactory.getLogger(OracleDataRepository.class);

    public OracleDataRepository(MigrationContext migrationContext,
            final DataSourceConfiguration dataSourceConfiguration,
            final DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(migrationContext, dataSourceConfiguration, databaseMigrationDataTypeMapperService);
        ensureJdbcCompliance();
    }

    private void ensureJdbcCompliance() {
        // without this types like timestamps may not be jdbc compliant
        System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");
        System.getProperties().setProperty("oracle.jdbc.autoCommitSpecCompliant", "false");
    }

    @Override
    protected DataSet convertToBatchDataSet(int batchId, final ResultSet resultSet) throws Exception {
        return convertToDataSet(batchId, resultSet, Collections.singleton("rn"));
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        // spotless:off
        return String.format(
                "select * " +
                        " from ( " +
                        " select /*+ first_rows(%s) */ " +
                        "  t.*, " +
                        "  row_number() " +
                        "  over (order by %s) rn " +
                        " from %s t where %s) " +
                        "where rn between %s and %s " +
                        "order by rn",
        // spotless:on
                queryDefinition.getBatchSize(), queryDefinition.getOrderByColumns(), queryDefinition.getTable(),
                expandConditions(conditions), queryDefinition.getOffset() + 1,
                queryDefinition.getOffset() + queryDefinition.getBatchSize());
    }

    // https://blogs.oracle.com/oraclemagazine/on-top-n-and-pagination-queries
    // "Pagination in Getting Rows N Through M"
    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        // spotless:off
        return String.format(
                "select * " +
                        " from ( " +
                        " select /*+ first_rows(%s) */ " +
                        "  t.*, " +
                        "  row_number() " +
                        "  over (order by t.%s) rn " +
                        " from %s t where %s) " +
                        "where rn <= %s " +
                        "order by rn",
        // spotless:on
                queryDefinition.getBatchSize(), queryDefinition.getColumn(), queryDefinition.getTable(),
                expandConditions(conditions), queryDefinition.getBatchSize());
    }

    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        String column = queryDefinition.getColumn();
        // spotless:off
        return String.format("SELECT t.%s, t.rownr as \"rownum\" \n" +
                "FROM\n" +
                "(\n" +
                "    SELECT %s, (ROW_NUMBER() OVER (ORDER BY %s))-1 AS rownr\n" +
                "    FROM %s\n WHERE %s" +
                ") t\n" +
                "WHERE mod(t.rownr,%s) = 0\n" +
                "ORDER BY t.%s",
        // spotless:om
                column, column, column, queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getBatchSize(), column);
    }

    @Override
    protected String createAllTableNamesQuery() {
        return String.format(
                "select distinct TABLE_NAME from ALL_TAB_COLUMNS where lower(OWNER) = lower('%s')",
                getDataSourceConfiguration().getSchema());
    }

    @Override
    protected String createAllColumnNamesQuery(String table) {
        return String.format(
                "select distinct COLUMN_NAME from ALL_TAB_COLUMNS where lower(OWNER) = lower('%s') AND lower(TABLE_NAME) = lower('%s')",
                getDataSourceConfiguration().getSchema(), table);
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        // spotless:off
        return String.format("SELECT t2.\"COLUMN_NAME\"\n" +
                "FROM\n" +
                "(\n" +
                "  SELECT * FROM (\n" +
                "    SELECT i.\"OWNER\", i.\"TABLE_NAME\", i.\"INDEX_NAME\", count(*) as \"COL_COUNT\"\n" +
                "    FROM ALL_INDEXES i\n" +
                "    INNER JOIN ALL_IND_COLUMNS c\n" +
                "    ON i.\"INDEX_NAME\" = c.\"INDEX_NAME\" AND i.\"OWNER\" = c.\"INDEX_OWNER\" AND i.\"TABLE_NAME\" = c.\"TABLE_NAME\"\n" +
                "    WHERE \n" +
                "    lower(i.\"OWNER\") = lower('%s')\n" +
                "    AND\n" +
                "    lower(i.\"TABLE_NAME\") = lower('%s')\n" +
                "    AND\n" +
                "    lower(i.\"UNIQUENESS\") = lower('UNIQUE')\n" +
                "    GROUP BY i.\"OWNER\", i.\"TABLE_NAME\", i.\"INDEX_NAME\"\n" +
                "    ORDER BY COL_COUNT ASC  \n" +
                "  )\n" +
                "  WHERE ROWNUM = 1\n" +
                ") t1\n" +
                "INNER JOIN ALL_IND_COLUMNS t2\n" +
                "ON t1.\"INDEX_NAME\" = t2.\"INDEX_NAME\" AND t1.\"OWNER\" = t2.\"INDEX_OWNER\" AND t1.\"TABLE_NAME\" = t2.\"TABLE_NAME\"", getDataSourceConfiguration().getSchema(), tableName);
        // spotless:on
    }

    @Override
    protected Platform createPlatform(final DatabaseSettings databaseSettings, final DataSource dataSource) {
        final HybrisPlatform platform = HybrisOraclePlatform.build(databaseSettings);
        /*
         * ORACLE_TARGET -> if the JdbcModelReader.readTables() is invoked with a null
         * schemaPattern, protected Collection readTables(String catalog, String
         * schemaPattern, String[] tableTypes) throws SQLException { ..then in Oracle it
         * retrieves ALL the tables ..include SYS. This causes other issues such as
         * Unsupported JDBC Type Exception, therefore always set the schema pattern to
         * the target Oracle's schema.
         */
        platform.getModelReader().setDefaultSchemaPattern(getDataSourceConfiguration().getSchema());
        platform.setDataSource(dataSource);
        return platform;
    }

    @Override
    public void runSqlScript(final Resource resource) {
        final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.setIgnoreFailedDrops(true);
        databasePopulator.setSeparator("/");
        databasePopulator.execute(getDataSource());

    }

    @Override
    public float getDatabaseUtilization() throws SQLException {
        return (float) 1.00;
    }

    @Override
    protected void addCustomPlatformTypeMapping(final Platform platform) {
        platform.getPlatformInfo().addNativeTypeMapping(Types.NVARCHAR, "VARCHAR2");
        platform.getPlatformInfo().setHasSize(Types.NVARCHAR, true);
        platform.getPlatformInfo().addNativeTypeMapping(Types.VARBINARY, "BLOB");
        platform.getPlatformInfo().setHasSize(Types.VARBINARY, false);

        platform.getPlatformInfo().addNativeTypeMapping(Types.REAL, "NUMBER(30,8)");
        platform.getPlatformInfo().setHasPrecisionAndScale(Types.REAL, false);

        platform.getPlatformInfo().addNativeTypeMapping(Types.DOUBLE, "NUMBER(30,8)");
        platform.getPlatformInfo().setHasPrecisionAndScale(Types.DOUBLE, false);
        platform.getPlatformInfo().setHasSize(Types.DOUBLE, false);

        platform.getPlatformInfo().addNativeTypeMapping(Types.BIGINT, "NUMBER(20,0)");
        platform.getPlatformInfo().setHasSize(Types.BIGINT, false);
        platform.getPlatformInfo().setHasPrecisionAndScale(Types.BIGINT, false);

        platform.getPlatformInfo().addNativeTypeMapping(Types.INTEGER, "NUMBER(20,0)");
        platform.getPlatformInfo().setHasSize(Types.INTEGER, false);
        platform.getPlatformInfo().setHasPrecisionAndScale(Types.INTEGER, false);

        platform.getPlatformInfo().addNativeTypeMapping(Types.TINYINT, "NUMBER(1,0)");
        platform.getPlatformInfo().setHasSize(Types.TINYINT, false);
        platform.getPlatformInfo().setHasPrecisionAndScale(Types.TINYINT, false);

        platform.getPlatformInfo().addNativeTypeMapping(Types.CHAR, "NUMBER(10,0)");
        platform.getPlatformInfo().setHasSize(Types.CHAR, false);
        platform.getPlatformInfo().setHasPrecisionAndScale(Types.CHAR, false);

    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.ORACLE;
    }

    @Override
    public String buildBulkUpsertStatement(String table, List<String> columnsToCopy, List<String> upsertIDs) {
        final StringBuilder sqlBuilder = new StringBuilder();

        sqlBuilder.append(String.format("MERGE INTO %s t", table));
        sqlBuilder.append("\n");
        sqlBuilder.append(String.format("USING (SELECT %s from dual) s ON (t.%s = s.%s)",
                Joiner.on(',').join(columnsToCopy.stream().map(column -> "? " + column).collect(Collectors.toList())),
                upsertIDs.get(0), upsertIDs.get(0)));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN MATCHED THEN UPDATE"); // update
        sqlBuilder.append("\n");
        sqlBuilder.append(getBulkUpdateStatementParamList(columnsToCopy,
                columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList()), upsertIDs));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN NOT MATCHED THEN INSERT"); // insert
        sqlBuilder.append("\n");
        sqlBuilder.append(getBulkInsertStatementParamList(columnsToCopy,
                columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));

        return sqlBuilder.toString();
    }

    @Override
    protected String getBulkInsertStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues) {
        return "(" + String.join(", ", columnsToCopy) + ") VALUES (" + String.join(", ", columnsToCopyValues) + ")";
    }

    @Override
    protected String getBulkUpdateStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues,
            List<String> upsertIDs) {
        final String upsertID = upsertIDs.get(0);
        final List<String> columnsToCopyMinusPK = columnsToCopy.stream().filter(s -> !s.equalsIgnoreCase(upsertID))
                .collect(Collectors.toList());
        final List<String> columnsToCopyValuesMinusPK = columnsToCopyValues.stream()
                .filter(s -> !s.equalsIgnoreCase("s." + upsertID)).collect(Collectors.toList());
        LOG.debug("getBulkUpdateStatementParamList - columnsToCopyMinusPK =" + columnsToCopyMinusPK);
        return "SET " + IntStream.range(0, columnsToCopyMinusPK.size()).mapToObj(
                idx -> String.format("%s = %s", columnsToCopyMinusPK.get(idx), columnsToCopyValuesMinusPK.get(idx)))
                .collect(Collectors.joining(", "));
    }

    @Override
    public String getDatabaseTimezone() {
        String query = "SELECT DBTIMEZONE FROM DUAL ";
        try (Connection conn = super.getConnection(); PreparedStatement stmt = conn.prepareStatement(query)) {
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                if (rs.getString("DBTIMEZONE").equals("+00:00"))
                    return "UTC";
                else
                    return "Different timezone";
            }
        } catch (Exception e) {
            e.getMessage();
        }
        return null;
    }
}
