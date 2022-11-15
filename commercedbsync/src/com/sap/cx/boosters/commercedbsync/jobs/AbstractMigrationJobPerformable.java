/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */
package com.sap.cx.boosters.commercedbsync.jobs;

import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.jalo.AbortCronJobException;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.AbstractJobPerformable;
import de.hybris.platform.servicelayer.cronjob.CronJobService;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import de.hybris.platform.util.Config;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import com.sap.cx.boosters.commercedbsync.context.IncrementalMigrationContext;
import com.sap.cx.boosters.commercedbsync.model.cron.FullMigrationCronJobModel;
import com.sap.cx.boosters.commercedbsync.model.cron.IncrementalMigrationCronJobModel;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Set;

public abstract class AbstractMigrationJobPerformable extends AbstractJobPerformable<CronJobModel> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMigrationJobPerformable.class);

    private static final String[] TYPE_SYSTEM_RELATED_TYPES = new String[]{"atomictypes", "attributeDescriptors", "collectiontypes", "composedtypes", "enumerationvalues", "maptypes"};

	private static final String MIGRATION_UPDATE_TYPE_SYSTEM = "migration.ds.update.typesystem.table";
    private static final String SOURCE_TYPESYSTEMNAME = "migration.ds.source.db.typesystemname";

    private static final String SOURCE_TYPESYSTEMSUFFIX = "migration.ds.source.db.typesystemsuffix";

    private static final String TYPESYSTEM_SELECT_STATEMENT = "IF (EXISTS (SELECT * \n" +
            "  FROM INFORMATION_SCHEMA.TABLES \n" +
            "  WHERE TABLE_SCHEMA = '%s' \n" +
            "  AND TABLE_NAME = '%2$s'))\n" +
            "BEGIN\n" +
            "  select name from %2$s where state = 'current'\n" +
            "END";


	protected DatabaseMigrationService databaseMigrationService;
	protected IncrementalMigrationContext incrementalMigrationContext;
    protected CronJobService cronJobService;
    protected String currentMigrationId;
    private  JdbcTemplate jdbcTemplate;

    @Override
    public boolean isPerformable()
    {
        for(CronJobModel cronJob : getCronJobService().getRunningOrRestartedCronJobs()){
            if ((cronJob instanceof IncrementalMigrationCronJobModel
                || cronJob instanceof FullMigrationCronJobModel)) {
                LOG.info("Previous migrations job already running {} and Type {} ", cronJob.getCode(), cronJob.getItemtype());
                return false;
            }
        }
        return true;
    }

	/*
	 * ORACLE_TARGET - START The updateTypesystemTabl() also updates the TS. There is scope to make these 2 update
	 * methods efficient i.e set the TS only once.
	 */

	protected void updateSourceTypesystemProperty() throws Exception
	{
		// Disabling Post processor
		Config.setParameter("migration.data.postprocessor.tscheck.disable", "yes");

		if(BooleanUtils.isFalse(Config.getBoolean(MIGRATION_UPDATE_TYPE_SYSTEM, false))){
			return;
		}
		DataRepository sourceRepository = incrementalMigrationContext.getDataSourceRepository();
		try(
			Connection connection = sourceRepository.getConnection();
			Statement stmt = connection.createStatement();
			ResultSet resultSet = stmt.executeQuery(String.format(TYPESYSTEM_SELECT_STATEMENT,
					sourceRepository.getDataSourceConfiguration().getSchema(), "CCV2_TYPESYSTEM_MIGRATIONS"));
			) {
			LOG.debug("SETTING the Type System from CCV2_TYPESYSTEM_MIGRATIONS" + String.format(TYPESYSTEM_SELECT_STATEMENT,
					sourceRepository.getDataSourceConfiguration().getSchema(), "CCV2_TYPESYSTEM_MIGRATIONS"));

			String typeSystemName = null;
			if (resultSet.next())
			{
				typeSystemName = resultSet.getString("name");
			}
			else
			{
				return;
			}
			if (typeSystemName != null && !typeSystemName.isEmpty())
			{
				Config.setParameter(SOURCE_TYPESYSTEMNAME, typeSystemName);
				LOG.info("SETTING typeSystemName = " + typeSystemName);
				return;
			}
		}
		}
    protected void updateTypesystemTable(Set<String> migrationItems) throws Exception {

		if(BooleanUtils.isFalse(Config.getBoolean(MIGRATION_UPDATE_TYPE_SYSTEM, false))){
			return;
		}
		DataRepository sourceRepository = incrementalMigrationContext.getDataSourceRepository();
		for(final String tableName: migrationItems){
            if(Arrays.stream(TYPE_SYSTEM_RELATED_TYPES).anyMatch(t -> StringUtils.startsWithIgnoreCase(tableName, t)))
            {
                try (
                         Connection connection = sourceRepository.getConnection();
                         Statement stmt = connection.createStatement();
                         ResultSet resultSet = stmt.executeQuery(String.format(TYPESYSTEM_SELECT_STATEMENT,
                                 sourceRepository.getDataSourceConfiguration().getSchema(),"CCV2_TYPESYSTEM_MIGRATIONS"));
				)
				{
					LOG.debug("Type System table - table found in list, get latest TS => " + String.format(TYPESYSTEM_SELECT_STATEMENT,
							sourceRepository.getDataSourceConfiguration().getSchema(), "CCV2_TYPESYSTEM_MIGRATIONS"));
                         String typeSystemName = null;
                        if (resultSet.next()) {
                            typeSystemName = resultSet.getString("name");;
                        } else{
                            return;
                        }

					final String tsBaseTableName = extractTSbaseTableName(tableName);

					LOG.info("Type System table - table found in list, get latest Table name " + String.format(
							"SELECT TableName FROM %s WHERE Typecode IS NOT NULL AND TableName LIKE '%s' AND TypeSystemName = '%s'",
							CommercedbsyncConstants.DEPLOYMENTS_TABLE, tsBaseTableName + "%", typeSystemName));
					final String typeSystemTablesQuery = String.format(
							"SELECT TableName FROM %s WHERE Typecode IS NOT NULL AND TableName LIKE '%s' AND TypeSystemName = '%s'",
							CommercedbsyncConstants.DEPLOYMENTS_TABLE, tsBaseTableName + "%", typeSystemName);
					final ResultSet typeSystemtableresultSet = stmt.executeQuery(typeSystemTablesQuery);
					String typeSystemTableName = null;
					if (typeSystemtableresultSet.next())
					{
						typeSystemTableName = typeSystemtableresultSet.getString("TableName");
					}
					// ORACLE_TARGET - START, add null check and return;
					if (typeSystemTableName != null)
					{
						Config.setParameter(SOURCE_TYPESYSTEMNAME, typeSystemName);
						final String typesystemsuffix = typeSystemTableName.substring(tsBaseTableName.length());

						Config.setParameter(SOURCE_TYPESYSTEMSUFFIX, typesystemsuffix);
						LOG.info("typeSystemName = " + typeSystemName + ",typesystemsuffix = " + typesystemsuffix);
						return;
					}
				}
			}
		}
	}

	/*
	 * If enumerationvalueslp, then extract enumerationvalues as base table name.
	 */
	private String extractTSbaseTableName(final String tableNameFromMigrationItems)
	{
		String tsBaseTableName = tableNameFromMigrationItems;

		// if it ends with lp
		if (tableNameFromMigrationItems.toLowerCase().endsWith("lp"))
		{
			tsBaseTableName = tableNameFromMigrationItems.substring(0, tableNameFromMigrationItems.length() - 2);
		}

		return tsBaseTableName;
	}

	protected MigrationStatus waitForFinishCronjobs(IncrementalMigrationContext context, String migrationID,
													final CronJobModel cronJobModel) throws Exception {
		MigrationStatus status;
		Thread.sleep(5000);
		boolean aborted = false;
		long since = 0;
		do {
			OffsetDateTime sinceTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(since), ZoneOffset.UTC);
			status = databaseMigrationService.getMigrationState(context, migrationID,sinceTime);
			Thread.sleep(5000);
			since = System.currentTimeMillis();
			if (isJobStateAborted(cronJobModel))
			{
				aborted = true;
				break;
			}
		} while (!status.isCompleted());

		if (aborted)
		{
			LOG.info(" Aborted ...STOPPING migration ");
			databaseMigrationService.stopMigration(incrementalMigrationContext, currentMigrationId);
			LOG.error("Database migration has been ABORTED, Migration State= " + status + ", Total Tasks "
					+ status.getTotalTasks() + ", migration id =" + status.getMigrationID() + ", Completed Tasks "
					+ status.getCompletedTasks());
			clearAbortRequestedIfNeeded(cronJobModel);
			throw new AbortCronJobException("CronJOB ABORTED");
		}

		if (status.isFailed()) {
			LOG.error("Database migration FAILED, Migration State= " + status + ", Total Tasks "
					+ status.getTotalTasks() + ", migration id =" + status.getMigrationID() + ", Completed Tasks "
					+ status.getCompletedTasks());
			throw new Exception("Database migration failed");
		}

		return status;
	}

	protected boolean isJobStateAborted(final CronJobModel cronJobModel)
	{
		this.modelService.refresh(cronJobModel);
		LOG.info("cron job status = " + cronJobModel.getStatus());
		LOG.info("cron job request to abort =" + cronJobModel.getRequestAbort());
		return ((cronJobModel.getStatus() == CronJobStatus.ABORTED)
				|| (cronJobModel.getRequestAbort() == null ? false : cronJobModel.getRequestAbort()));
	}

    @Override
    public boolean isAbortable() {
        return true;
    }

    public IncrementalMigrationContext getIncrementalMigrationContext() {
        return incrementalMigrationContext;
    }

    public void setIncrementalMigrationContext(IncrementalMigrationContext incrementalMigrationContext) {
        this.incrementalMigrationContext = incrementalMigrationContext;
    }

    public CronJobService getCronJobService() {
        return cronJobService;
    }

    public void setCronJobService(CronJobService cronJobService) {
        this.cronJobService = cronJobService;
    }

    public DatabaseMigrationService getDatabaseMigrationService() {
        return databaseMigrationService;
    }

    public void setDatabaseMigrationService(DatabaseMigrationService databaseMigrationService) {
        this.databaseMigrationService = databaseMigrationService;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
}
