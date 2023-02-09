/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import javax.sql.DataSource;

import org.apache.ddlutils.Platform;
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

public class PostGresDataRepository extends AbstractDataRepository {
    public PostGresDataRepository(DataSourceConfiguration dataSourceConfiguration, DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        return String.format("select * from %s where %s order by %s limit %s,%s", queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getOrderByColumns(), queryDefinition.getOffset(), queryDefinition.getBatchSize());
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
        return String.format("select * from %s where %s order by %s limit %s", queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getColumn(), queryDefinition.getBatchSize());
    }


    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        String column = queryDefinition.getColumn();
        return String.format("SELECT %s,rownum\n" +
                "FROM ( \n" +
                "    SELECT \n" +
                "        @row := @row +1 AS rownum, %s \n" +
                "    FROM (SELECT @row :=-1) r, %s  WHERE %s ORDER BY %s) ranked \n" +
                "WHERE rownum %% %s = 0 ", column, column, queryDefinition.getTable(), expandConditions(conditions), column, queryDefinition.getBatchSize());
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
    protected Platform createPlatform(DatabaseSettings databaseSettings, DataSource dataSource) {
        HybrisPlatform instance = MigrationHybrisPostGresPlatform.build(databaseSettings);
        instance.setDataSource(dataSource);
        return instance;
    }
}
