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

    private static final String MIGRATION_DS_TARGET_DB_URL = "migration.ds.target.db.url";
    private static final String COMMERCE_DB_URL = "db.url";
    private static final String DISABLE_UNLOCKING = "system.unlocking.disabled";
    private ConfigurationService configurationService;

    @Override
    public void validateContext(final MigrationContext context) {
        final String migrationTargetDbUrl = getConfigurationService().getConfiguration().getString(MIGRATION_DS_TARGET_DB_URL);
        final String commerceDbUrl = getConfigurationService().getConfiguration().getString(COMMERCE_DB_URL);
        final boolean isSystemLocked = getConfigurationService().getConfiguration().getBoolean(DISABLE_UNLOCKING);
        if (migrationTargetDbUrl.equals(commerceDbUrl) && isSystemLocked) {
            throw new RuntimeException("Unable to run migration on a locked system. Check property " + DISABLE_UNLOCKING);
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