/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.lang3.StringUtils;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

public class AdjustActiveTypeSystemPostProcessor implements MigrationPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AdjustActiveTypeSystemPostProcessor.class.getName());

    private static final String CCV2_TS_MIGRATION_TABLE = "CCV2_TYPESYSTEM_MIGRATIONS";
    private static final String TYPESYSTEM_ADJUST_STATEMENT = "IF (EXISTS (SELECT * \n" +
            "  FROM INFORMATION_SCHEMA.TABLES \n" +
            "  WHERE TABLE_SCHEMA = '%s' \n" +
            "  AND TABLE_NAME = '%3$s'))\n" +
            "BEGIN\n" +
            "  UPDATE [%3$s] SET [state] = 'retired' WHERE 1=1;\n" +
            "  UPDATE [%3$s] SET [state] = 'current', [comment] = 'Updated by CMT' WHERE [name] = '%s';\n" +
            "END";
	// ORACLR_TARGET - START
	private static final String[] TRUEVALUES = new String[] { "yes", "y", "true", "0" };
	private static final String CMT_DISABLED_POST_PROCESSOR = "migration.data.postprocessor.tscheck.disable";
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

		if (isPostProcesorDisabled()) {
			LOG.info("TS post processor is disabled ");
			return;
		}
		final DataRepository targetRepository = context.getMigrationContext().getDataTargetRepository();
		final String typeSystemName = targetRepository.getDataSourceConfiguration().getTypeSystemName();

		try ( Connection connection = targetRepository.getConnection();
		PreparedStatement  statement = connection.prepareStatement(String.format(TYPESYSTEM_ADJUST_STATEMENT,
				targetRepository.getDataSourceConfiguration().getSchema(), typeSystemName,
				getMigrationsTableName(targetRepository)));
		) {
			statement.execute();
			
			LOG.info("Adjusted active type system to: " + typeSystemName);
		} catch (SQLException e) {
			LOG.error("Error executing post processor (SQLException) ", e);
		} catch (Exception e) {
			LOG.error("Error executing post processor", e);
		}
	}

	private String getMigrationsTableName(final DataRepository repository) {
		return StringUtils.trimToEmpty(repository.getDataSourceConfiguration().getTablePrefix())
				.concat(CCV2_TS_MIGRATION_TABLE);
	}

	private boolean isPostProcesorDisabled() {
		final String ccv2DisabledProperties = getConfigurationService().getConfiguration()
				.getString(CMT_DISABLED_POST_PROCESSOR);
		// boolean disabled = false;
		if (ccv2DisabledProperties == null || ccv2DisabledProperties.isEmpty()) {
			return false;
		}
		return Arrays.stream(TRUEVALUES).anyMatch(ccv2DisabledProperties::equalsIgnoreCase);
		// return disabled;
	}
}
