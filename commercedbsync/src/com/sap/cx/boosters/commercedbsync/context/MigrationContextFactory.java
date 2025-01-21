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
    final boolean reversed;

    public MigrationContextFactory(final DataRepositoryFactory dataRepositoryFactory,
            final DataSourceConfigurationFactory dataSourceConfigurationFactory, final Configuration configuration,
            final boolean reversed) {
        this.dataRepositoryFactory = dataRepositoryFactory;
        this.dataSourceConfigurationFactory = dataSourceConfigurationFactory;
        this.configuration = configuration;
        this.reversed = reversed;
    }

    public MigrationContext create() throws Exception {
        if (configuration.getBoolean(CommercedbsyncConstants.MIGRATION_DATA_SYNCHRONIZATION_ENABLED, false)) {
            return new DefaultIncrementalMigrationContext(dataRepositoryFactory, dataSourceConfigurationFactory,
                    configuration, reversed);
        }

        return new DefaultMigrationContext(dataRepositoryFactory, dataSourceConfigurationFactory, configuration, false);
    }
}
