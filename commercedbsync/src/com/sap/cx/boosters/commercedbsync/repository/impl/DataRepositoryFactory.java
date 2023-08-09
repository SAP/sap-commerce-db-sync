/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import com.google.common.base.Strings;
import com.sap.cx.boosters.commercedbsync.context.IncrementalMigrationContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public class DataRepositoryFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DataRepositoryFactory.class);

    protected final DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService;

    public DataRepositoryFactory(DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        this.databaseMigrationDataTypeMapperService = databaseMigrationDataTypeMapperService;
    }

    public DataRepository create(MigrationContext migrationContext,
            Set<DataSourceConfiguration> dataSourceConfigurations) throws Exception {
        Objects.requireNonNull(dataSourceConfigurations);
        if (dataSourceConfigurations.isEmpty()) {
            return new NullRepository("no datasource specified", null);
        }
        Set<DataRepository> repositories = new HashSet<>();
        for (DataSourceConfiguration dataSourceConfiguration : dataSourceConfigurations) {
            try {
                repositories.add(doCreate(dataSourceConfiguration, migrationContext));
            } catch (Exception e) {
                LOG.error("Error creating data repository", e);
                repositories.add(new NullRepository(e.getMessage(), dataSourceConfiguration));
            }
        }
        if (repositories.size() > 1) {
            // TODO implement a CompositeRepository to handle multiple inputs/outputs
            return new NullRepository("multiple data source profiles as input/output is currently not supported", null);
        } else {
            return repositories.stream().findFirst()
                    .orElseThrow(() -> new NoSuchElementException("The element being requested does not exist."));
        }
    }

    protected DataRepository doCreate(DataSourceConfiguration dataSourceConfiguration,
            MigrationContext migrationContext) throws Exception {
        String connectionString = dataSourceConfiguration.getConnectionString();
        if (Strings.isNullOrEmpty(connectionString)) {
            throw new RuntimeException(
                    "No connection string provided for data source '" + dataSourceConfiguration.getProfile() + "'");
        } else {
            String connectionStringLower = connectionString.toLowerCase();
            boolean incremental = isIncremental(dataSourceConfiguration, migrationContext);

            if (connectionStringLower.startsWith("jdbc:mysql")) {
                if (incremental) {
                    return new MySQLIncrementalDataRepository(migrationContext, dataSourceConfiguration,
                            databaseMigrationDataTypeMapperService);
                }

                return new MySQLDataRepository(migrationContext, dataSourceConfiguration,
                        databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:sqlserver")) {
                if (incremental) {
                    return new AzureIncrementalDataRepository(migrationContext, dataSourceConfiguration,
                            databaseMigrationDataTypeMapperService);
                }

                return new AzureDataRepository(migrationContext, dataSourceConfiguration,
                        databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:oracle")) {
                return new OracleDataRepository(migrationContext, dataSourceConfiguration,
                        databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:sap")) {
                return new HanaDataRepository(migrationContext, dataSourceConfiguration,
                        databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:hsqldb")) {
                return new HsqlRepository(migrationContext, dataSourceConfiguration,
                        databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:postgresql")) {
                return new PostGresDataRepository(migrationContext, dataSourceConfiguration,
                        databaseMigrationDataTypeMapperService);
            }
        }
        throw new RuntimeException("Cannot handle connection string for " + connectionString);
    }

    protected boolean isIncremental(DataSourceConfiguration dataSourceConfiguration,
            MigrationContext migrationContext) {
        return migrationContext instanceof IncrementalMigrationContext
                && migrationContext.getInputProfiles().contains(dataSourceConfiguration.getProfile());
    }
}
