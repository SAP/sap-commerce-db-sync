/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import com.google.common.base.Strings;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;

public class DataRepositoryFactory {

    protected final DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService;

    public DataRepositoryFactory(DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        this.databaseMigrationDataTypeMapperService = databaseMigrationDataTypeMapperService;
    }

    public DataRepository create(DataSourceConfiguration dataSourceConfiguration)
            throws Exception {
        String connectionString = dataSourceConfiguration.getConnectionString();
        if (Strings.isNullOrEmpty(connectionString)) {
            throw new RuntimeException("No connection string provided for data source '" + dataSourceConfiguration.getProfile() + "'");
        } else {
            String connectionStringLower = connectionString.toLowerCase();
            if (connectionStringLower.startsWith("jdbc:mysql")) {
                return new MySQLDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:sqlserver")) {
                return new AzureDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:oracle")) {
                return new OracleDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:sap")) {
                return new HanaDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:hsqldb")) {
                return new HsqlRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:postgresql")) {
                return new PostGresDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:postgresql")) {
                return new PostGresDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            }
        }
        throw new RuntimeException("Cannot handle connection string for " + connectionString);
    }
}
