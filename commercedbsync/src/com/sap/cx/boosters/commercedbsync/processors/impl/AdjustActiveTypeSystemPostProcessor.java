/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AdjustActiveTypeSystemPostProcessor implements MigrationPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AdjustActiveTypeSystemPostProcessor.class.getName());

    private static final String CCV2_TS_MIGRATION_TABLE = "CCV2_TYPESYSTEM_MIGRATIONS";
    // spotless:off
    private static final String TYPESYSTEM_ADJUST_STATEMENT = "IF (EXISTS (SELECT * \n" +
            "  FROM INFORMATION_SCHEMA.TABLES \n" +
            "  WHERE TABLE_SCHEMA = '%s' \n" +
            "  AND TABLE_NAME = '%3$s'))\n" +
            "BEGIN\n" +
            "  UPDATE [%3$s] SET [state] = 'retired' WHERE 1=1;\n" +
            "  UPDATE [%3$s] SET [state] = 'current', [comment] = 'Updated by CMT' WHERE [name] = '%s';\n" +
            "END";
    // spotless:on
    private static final String TS_CHECK_POST_PROCESSOR_DISABLED = "migration.data.postprocessor.tscheck.disable";
    private ConfigurationService configurationService;

    /**
     * @return the configurationService
     */
    public ConfigurationService getConfigurationService() {
        return configurationService;
    }

    /**
     * @param configurationService
     *            the configurationService to set
     */
    public void setConfigurationService(final ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Override
    public void process(final CopyContext context) {
        final DataRepository targetRepository = context.getMigrationContext().getDataTargetRepository();
        final String typeSystemName = targetRepository.getDataSourceConfiguration().getTypeSystemName();

        try (Connection connection = targetRepository.getConnection();
                PreparedStatement statement = connection.prepareStatement(String.format(TYPESYSTEM_ADJUST_STATEMENT,
                        targetRepository.getDataSourceConfiguration().getSchema(), typeSystemName,
                        getMigrationsTableName(targetRepository)))) {
            statement.execute();

            LOG.info("Adjusted active type system to: " + typeSystemName);
        } catch (SQLException e) {
            LOG.error("Error executing post processor (SQLException) ", e);
        } catch (Exception e) {
            LOG.error("Error executing post processor", e);
        }
    }

    @Override
    public boolean shouldExecute(CopyContext context) {
        return !isPostProcesorDisabled() && !context.getMigrationContext().isDataSynchronizationEnabled()
                && context.getMigrationContext().getDataTargetRepository().getDatabaseProvider().isMssqlUsed();
    }

    private String getMigrationsTableName(final DataRepository repository) {
        return StringUtils.trimToEmpty(repository.getDataSourceConfiguration().getTablePrefix())
                .concat(CCV2_TS_MIGRATION_TABLE);
    }

    private boolean isPostProcesorDisabled() {
        return BooleanUtils.toBoolean(
                getConfigurationService().getConfiguration().getString(TS_CHECK_POST_PROCESSOR_DISABLED, "false"));
    }
}
