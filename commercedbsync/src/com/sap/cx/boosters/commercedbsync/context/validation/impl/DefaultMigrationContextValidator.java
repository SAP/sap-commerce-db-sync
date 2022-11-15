/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context.validation.impl;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.context.validation.MigrationContextValidator;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.lang.StringUtils;

import java.util.Locale;

public class DefaultMigrationContextValidator implements MigrationContextValidator {

    private static final String DB_URL_PROPERTY_KEY = "migration.ds.target.db.url";
    private static final String DISABLE_UNLOCKING = "system.unlocking.disabled";
    private ConfigurationService configurationService;

    @Override
    public void validateContext(final MigrationContext context) {
        // Canonically the target should always be the CCV2 DB and we have to verify nobody is trying to copy *from* that
        final String sourceDbUrl = context.getDataSourceRepository().getDataSourceConfiguration().getConnectionString();
        final String ccv2ManagedDB = getConfigurationService().getConfiguration().getString(DB_URL_PROPERTY_KEY);
        final boolean isSystemLocked = getConfigurationService().getConfiguration().getBoolean(DISABLE_UNLOCKING);

        if (sourceDbUrl.equals(ccv2ManagedDB)) {
            throw new RuntimeException("Invalid data source configuration - cannot use the CCV2-managed database as the source.");
        }

        if (isSystemLocked) {
            throw new RuntimeException("You cannot run the migration on locked system. Check property " + DISABLE_UNLOCKING);
        }

        //we check this for locale related comparison
        Locale defaultLocale = Locale.getDefault();
        if (defaultLocale == null || StringUtils.isEmpty(defaultLocale.toString())) {
            throw new RuntimeException("There is no default locale specified on the running server. Set the default locale and try again.");
        }
    }

    public ConfigurationService getConfigurationService() {
        return configurationService;
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

}
