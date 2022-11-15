/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;

public class HsqlRepository extends AbstractDataRepository {

    public HsqlRepository(DataSourceConfiguration dataSourceConfiguration, DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected String createAllTableNamesQuery() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String createAllColumnNamesQuery(String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.HSQL;
    }
}
