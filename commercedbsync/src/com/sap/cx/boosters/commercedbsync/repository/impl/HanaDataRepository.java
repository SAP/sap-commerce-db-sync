/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import com.google.common.base.Joiner;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.repository.platform.MigrationHybrisHANAPlatform;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatform;
import org.apache.ddlutils.Platform;
import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.sql.Types;

public class HanaDataRepository extends AbstractDataRepository {

    public HanaDataRepository(DataSourceConfiguration dataSourceConfiguration, DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        String orderBy = Joiner.on(',').join(queryDefinition.getAllColumns());
        return String.format("select * from %s where %s order by %s limit %s offset %s", queryDefinition.getTable(), expandConditions(conditions), orderBy, queryDefinition.getBatchSize(), queryDefinition.getOffset());
    }

    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        return String.format("select * from %s where %s order by %s limit %s", queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getColumn(), queryDefinition.getBatchSize());
    }

    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        String column = queryDefinition.getColumn();
        return String.format("SELECT t.%s, t.rownr as \"rownum\" \n" +
                "FROM\n" +
                "(\n" +
                "    SELECT %s, (ROW_NUMBER() OVER (ORDER BY %s))-1 AS rownr\n" +
                "    FROM %s\n WHERE %s" +
                ") t\n" +
                "WHERE mod(t.rownr,%s) = 0\n" +
                "ORDER BY t.%s", column, column, column, queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getBatchSize(), column);
    }

    @Override
    protected String createAllTableNamesQuery() {
        return String.format("select distinct table_name from table_columns where lower(schema_name) = lower('%s') order by table_name", getDataSourceConfiguration().getSchema());
    }

    @Override
    protected String createAllColumnNamesQuery(String table) {
        return String.format("select distinct column_name from table_columns where lower(schema_name) = lower('%s') and lower(table_name) = lower('%s')", getDataSourceConfiguration().getSchema(), table);
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
        return String.format("SELECT t2.\"COLUMN_NAME\"\n" +
                "FROM\n" +
                "(\n" +
                "  SELECT * FROM (\n" +
                "    SELECT i.\"SCHEMA_NAME\", i.\"TABLE_NAME\", i.\"INDEX_NAME\", count(*) as \"COL_COUNT\"\n" +
                "    FROM INDEXES i\n" +
                "    INNER JOIN INDEX_COLUMNS c\n" +
                "    ON i.\"INDEX_NAME\" = c.\"INDEX_NAME\" AND i.\"SCHEMA_NAME\" = c.\"SCHEMA_NAME\" AND i.\"TABLE_NAME\" = c.\"TABLE_NAME\"\n" +
                "    WHERE \n" +
                "    lower(i.\"SCHEMA_NAME\") = lower('%s')\n" +
                "    AND\n" +
                "    lower(i.\"TABLE_NAME\") = lower('%s')\n" +
                "    AND(\n" +
                "    lower(i.\"CONSTRAINT\") = lower('UNIQUE') OR \n" +
                "    lower(i.\"CONSTRAINT\") = lower('PRIMARY KEY'))\n" +
                "    GROUP BY i.\"SCHEMA_NAME\", i.\"TABLE_NAME\", i.\"INDEX_NAME\"\n" +
                "    ORDER BY COL_COUNT ASC  \n" +
                "  )\n" +
                "  LIMIT 1\n" +
                ") t1\n" +
                "INNER JOIN INDEX_COLUMNS t2\n" +
                "ON t1.\"INDEX_NAME\" = t2.\"INDEX_NAME\" AND t1.\"SCHEMA_NAME\" = t2.\"SCHEMA_NAME\" AND t1.\"TABLE_NAME\" = t2.\"TABLE_NAME\"", getDataSourceConfiguration().getSchema(), tableName);
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
    protected Platform createPlatform(DatabaseSettings databaseSettings, DataSource dataSource) {
        HybrisPlatform instance = MigrationHybrisHANAPlatform.build(databaseSettings);
        instance.setDataSource(dataSource);
        return instance;
    }
}

