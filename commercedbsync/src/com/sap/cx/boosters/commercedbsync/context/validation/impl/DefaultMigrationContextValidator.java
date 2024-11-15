/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context.validation.impl;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.context.validation.MigrationContextValidator;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import de.hybris.bootstrap.ddl.tools.persistenceinfo.PersistenceInformation;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Locale;

import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.DEPLOYMENTS_TABLE;

public class DefaultMigrationContextValidator implements MigrationContextValidator {

    private static final String DB_URL_PROPERTY_KEY = "db.url";
    private static final String DISABLE_UNLOCKING = "system.unlocking.disabled";
    private ConfigurationService configurationService;

    @Override
    public void validateContext(final MigrationContext context) {
        checkSourceDbIsNotTargetDb(context);
        checkSystemNotLocked(context);
        checkDefaultLocaleExists();
        checkTypeSystem(context.getDataSourceRepository());
        checkTypeSystem(context.getDataTargetRepository());
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

    private void checkTypeSystem(DataRepository dataRepository) {
        final DataSourceConfiguration configuration = dataRepository.getDataSourceConfiguration();
        if (configuration.getTypeSystemName().equals(PersistenceInformation.DEFAULT_TYPE_SYSTEM_NAME)) {
            return;
        }

        final String profile = configuration.getProfile();
        final String sql = "SELECT TableName FROM %s WHERE LOWER(TableName) LIKE ? AND TypeSystemName = ?";
        final String tableName = "composedtypes";
        String ydeploymentsTableName = configuration.getTablePrefix() + DEPLOYMENTS_TABLE;
        String typeSystemSuffix = StringUtils.EMPTY;
        boolean typeSystemExists = false;

        try (Connection connection = dataRepository.getConnection()) {
            if (connection.getMetaData().storesLowerCaseIdentifiers()) {
                ydeploymentsTableName = ydeploymentsTableName.toLowerCase();
            } else if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                ydeploymentsTableName = ydeploymentsTableName.toUpperCase();
            }

            try (PreparedStatement stmt = connection.prepareStatement(String.format(sql, ydeploymentsTableName))) {
                stmt.setString(1, tableName + '%');
                stmt.setString(2, configuration.getTypeSystemName());
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    typeSystemSuffix = rs.getString(1).substring(tableName.length());
                    typeSystemExists = true;
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Could not validate type system suffix against value inferred from " + profile
                    + " database: " + e.getMessage());
        }

        if (!typeSystemExists) {
            throw new RuntimeException("Type system [" + configuration.getTypeSystemName() + "] does not exists on "
                    + profile + " database");
        }

        if (!typeSystemSuffix.equals(configuration.getTypeSystemSuffix())) {
            throw new RuntimeException("Type system suffix in " + profile + " database does not match expected value: "
                    + typeSystemSuffix);
        }
    }

    public ConfigurationService getConfigurationService() {
        return configurationService;
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

}
