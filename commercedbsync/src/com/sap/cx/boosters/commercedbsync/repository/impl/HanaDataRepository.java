/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
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
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.repository.platform.MigrationHybrisHANAPlatform;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;

import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatform;

public class HanaDataRepository extends AbstractDataRepository {
    private static final Logger LOG = LoggerFactory.getLogger(HanaDataRepository.class);

    public HanaDataRepository(MigrationContext migrationContext, DataSourceConfiguration dataSourceConfiguration,
            DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(migrationContext, dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        return String.format("select * from %s where %s order by %s limit %s offset %s", queryDefinition.getTable(),
                expandConditions(conditions), queryDefinition.getOrderByColumns(), queryDefinition.getBatchSize(),
                queryDefinition.getOffset());
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
        return String.format("SELECT t.%s, t.rownr as \"rownum\" \n" +
                "FROM\n" +
                "(\n" +
                "    SELECT %s, (ROW_NUMBER() OVER (ORDER BY %s))-1 AS rownr\n" +
                "    FROM %s\n WHERE %s" +
                ") t\n" +
                "WHERE mod(t.rownr,%s) = 0\n" +
                "ORDER BY t.%s",
        // spotless:on
                column, column, column, queryDefinition.getTable(), expandConditions(conditions),
                queryDefinition.getBatchSize(), column);
    }

    @Override
    protected String createAllTableNamesQuery() {
        return String.format(
                "select distinct table_name from table_columns where lower(schema_name) = lower('%s') order by table_name",
                getDataSourceConfiguration().getSchema());
    }

    @Override
    protected String createAllColumnNamesQuery(String table) {
        return String.format(
                "select distinct column_name from table_columns where lower(schema_name) = lower('%s') and lower(table_name) = lower('%s')",
                getDataSourceConfiguration().getSchema(), table);
    }

    @Override
    public String getDatabaseTimezone() {
        String query = "select * from M_HOST_INFORMATION where upper(KEY) like '%TIMEZONE_NAME%'";
        try (Connection conn = super.getConnection(); PreparedStatement stmt = conn.prepareStatement(query)) {
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getString("VALUE");
            }
        } catch (Exception e) {
            e.getMessage();
        }
        return null;
    }

    @Override
    public void runSqlScript(Resource resource) {
        final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.setIgnoreFailedDrops(true);
        databasePopulator.setSeparator("#");
        databasePopulator.execute(getDataSource());
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        // spotless:off
		return String.format("SELECT t2.\"COLUMN_NAME\"\n" + "FROM\n" + "(\n" + "  SELECT * FROM (\n"
				+ "    SELECT i.\"SCHEMA_NAME\", i.\"TABLE_NAME\", i.\"INDEX_NAME\", count(*) as \"COL_COUNT\"\n"
				+ "    FROM INDEXES i\n" + "    INNER JOIN INDEX_COLUMNS c\n"
				+ "    ON i.\"INDEX_NAME\" = c.\"INDEX_NAME\" AND i.\"SCHEMA_NAME\" = c.\"SCHEMA_NAME\" AND i.\"TABLE_NAME\" = c.\"TABLE_NAME\"\n"
				+ "    WHERE \n" + "    lower(i.\"SCHEMA_NAME\") = lower('%s')\n" + "    AND\n"
				+ "    lower(i.\"TABLE_NAME\") = lower('%s')\n" + "    AND(\n"
				+ "    lower(i.\"CONSTRAINT\") = lower('UNIQUE') OR \n"
				+ "    lower(i.\"CONSTRAINT\") = lower('PRIMARY KEY'))\n"
				+ "    GROUP BY i.\"SCHEMA_NAME\", i.\"TABLE_NAME\", i.\"INDEX_NAME\"\n"
				+ "    ORDER BY COL_COUNT ASC  \n" + "  )\n" + "  LIMIT 1\n" + ") t1\n"
				+ "INNER JOIN INDEX_COLUMNS t2\n"
				+ "ON t1.\"INDEX_NAME\" = t2.\"INDEX_NAME\" AND t1.\"SCHEMA_NAME\" = t2.\"SCHEMA_NAME\" AND t1.\"TABLE_NAME\" = t2.\"TABLE_NAME\"",
        // spotless:on
                getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    protected void addCustomPlatformTypeMapping(final Platform platform) {
        platform.getPlatformInfo().addNativeTypeMapping(Types.NCHAR, "NVARCHAR", Types.NVARCHAR);
        platform.getPlatformInfo().addNativeTypeMapping(Types.CHAR, "VARCHAR", Types.VARCHAR);
        platform.getPlatformInfo().addNativeTypeMapping(Types.DOUBLE, "DECIMAL", Types.DECIMAL);
        // platform.getPlatformInfo().addNativeTypeMapping(-1, "NCLOB", Types.NCLOB);
    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.HANA;
    }

    @Override
    public String buildBulkUpsertStatement(String table, List<String> columnsToCopy, List<String> upsertIDs) {
        final StringBuilder sqlBuilder = new StringBuilder();

        sqlBuilder.append(String.format("MERGE INTO %s t", table));
        sqlBuilder.append("\n");
        sqlBuilder.append(String.format("USING (SELECT %s from dummy) s ON ",
                Joiner.on(',').join(columnsToCopy.stream().map(column -> "? " + column).collect(Collectors.toList()))));
        sqlBuilder.append(String.format("( %s )", upsertIDs.stream()
                .map(column -> String.format(" t.%s = s.%s", column, column)).collect(Collectors.joining(" AND "))));
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
        final String upsertID = upsertIDs.get(0); // TODO handle multiple upsert IDs if needed
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
    protected Platform createPlatform(DatabaseSettings databaseSettings, DataSource dataSource) {
        HybrisPlatform instance = MigrationHybrisHANAPlatform.build(databaseSettings);
        instance.setDataSource(dataSource);
        return instance;
    }
}
