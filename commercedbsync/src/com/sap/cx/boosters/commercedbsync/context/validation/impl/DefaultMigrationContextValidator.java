/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
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

    private static final String DB_URL_PROPERTY_KEY = "db.url";
    private static final String DISABLE_UNLOCKING = "system.unlocking.disabled";
    private ConfigurationService configurationService;

    @Override
    public void validateContext(final MigrationContext context) {
        checkSourceDbIsNotTargetDb(context);
        checkSystemNotLocked(context);
        checkDefaultLocaleExists();
    }

    private void checkSourceDbIsNotTargetDb(MigrationContext context) {
        if (context.isDataExportEnabled()) {
            return; // in this mode, source DB can (should?) be set to CCv2 instance
        }

        // Canonically the target should always be the CCV2 DB and we have to verify
        // nobody is trying to copy *from* that
        final String sourceDbUrl = context.getDataSourceRepository().getDataSourceConfiguration().getConnectionString();
        final String ccv2ManagedDB = getConfigurationService().getConfiguration().getString(DB_URL_PROPERTY_KEY);

        if (sourceDbUrl.equals(ccv2ManagedDB)) {
            throw new RuntimeException(
                    "Invalid data source configuration - cannot use the CCV2-managed database as the source.");
        }
    }

    private void checkSystemNotLocked(MigrationContext context) {
        final boolean isSystemLocked = getConfigurationService().getConfiguration().getBoolean(DISABLE_UNLOCKING);
        if (!context.isDataExportEnabled() && isSystemLocked) {
            throw new RuntimeException(
                    "You cannot run the migration on locked system. Check property " + DISABLE_UNLOCKING);
        }
    }

    private void checkDefaultLocaleExists() {
        // we check this for locale related comparison
        Locale defaultLocale = Locale.getDefault();
        if (defaultLocale == null || StringUtils.isEmpty(defaultLocale.toString())) {
            throw new RuntimeException(
                    "There is no default locale specified on the running server. Set the default locale and try again.");
        }
    }

    public ConfigurationService getConfigurationService() {
        return configurationService;
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

}
