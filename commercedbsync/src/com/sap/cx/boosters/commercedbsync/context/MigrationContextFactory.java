/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context;

import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.impl.DefaultIncrementalMigrationContext;
import com.sap.cx.boosters.commercedbsync.context.impl.DefaultMigrationContext;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfigurationFactory;
import com.sap.cx.boosters.commercedbsync.repository.impl.DataRepositoryFactory;
import org.apache.commons.configuration.Configuration;

public class MigrationContextFactory {
    final DataRepositoryFactory dataRepositoryFactory;
    final DataSourceConfigurationFactory dataSourceConfigurationFactory;
    final Configuration configuration;

    public MigrationContextFactory(DataRepositoryFactory dataRepositoryFactory,
            DataSourceConfigurationFactory dataSourceConfigurationFactory, Configuration configuration) {
        this.dataRepositoryFactory = dataRepositoryFactory;
        this.dataSourceConfigurationFactory = dataSourceConfigurationFactory;
        this.configuration = configuration;
    }

    public MigrationContext create() throws Exception {
        if (configuration.getBoolean(CommercedbsyncConstants.MIGRATION_DATA_EXPORT_ENABLED, false)) {
            return new DefaultIncrementalMigrationContext(dataRepositoryFactory, dataSourceConfigurationFactory,
                    configuration);
        }

        return new DefaultMigrationContext(dataRepositoryFactory, dataSourceConfigurationFactory, configuration);
    }
}
