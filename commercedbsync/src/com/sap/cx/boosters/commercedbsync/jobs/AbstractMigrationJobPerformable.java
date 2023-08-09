/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.jobs;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import com.sap.cx.boosters.commercedbsync.context.LaunchOptions;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.model.cron.MigrationCronJobModel;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.IncrementalMigrationContext;
import com.sap.cx.boosters.commercedbsync.model.cron.FullMigrationCronJobModel;
import com.sap.cx.boosters.commercedbsync.model.cron.IncrementalMigrationCronJobModel;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationService;

import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.jalo.AbortCronJobException;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.AbstractJobPerformable;
import de.hybris.platform.servicelayer.cronjob.CronJobService;
import de.hybris.platform.util.Config;

public abstract class AbstractMigrationJobPerformable extends AbstractJobPerformable<CronJobModel> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMigrationJobPerformable.class);

    private static final String[] RUNNING_MIGRATION = new String[]{MigrationProgress.RUNNING.toString(),
            MigrationProgress.PROCESSED.toString(), MigrationProgress.POSTPROCESSING.toString()};
    private static final String[] TYPE_SYSTEM_RELATED_TYPES = new String[]{"atomictypes", "attributeDescriptors",
            "collectiontypes", "composedtypes", "enumerationvalues", "maptypes"};

    private static final String MIGRATION_UPDATE_TYPE_SYSTEM = "migration.ds.update.typesystem.table";
    private static final String SOURCE_TYPESYSTEMNAME = "migration.ds.source.db.typesystemname";

    private static final String SOURCE_TYPESYSTEMSUFFIX = "migration.ds.source.db.typesystemsuffix";
    // spotless:off
    private static final String TYPESYSTEM_SELECT_STATEMENT = "IF (EXISTS (SELECT * \n" +
            "  FROM INFORMATION_SCHEMA.TABLES \n" +
            "  WHERE TABLE_SCHEMA = '%s' \n" +
            "  AND TABLE_NAME = '%2$s'))\n" +
            "BEGIN\n" +
            "  select name from %2$s where state = 'current'\n" +
            "END";
    // spotless:on

    protected DatabaseMigrationService databaseMigrationService;
    protected MigrationContext migrationContext;
    protected CronJobService cronJobService;
    protected String currentMigrationId;
    private JdbcTemplate jdbcTemplate;

    @Override
    public boolean isPerformable() {
        for (CronJobModel cronJob : getCronJobService().getRunningOrRestartedCronJobs()) {
            if ((cronJob instanceof IncrementalMigrationCronJobModel || cronJob instanceof FullMigrationCronJobModel)) {
                LOG.info("Previous migrations job already running {} and Type {} ", cronJob.getCode(),
                        cronJob.getItemtype());
                return false;
            }
        }
        return true;
    }

    /*
     * ORACLE_TARGET - START The updateTypesystemTabl() also updates the TS. There
     * is scope to make these 2 update methods efficient i.e set the TS only once.
     */

    protected void updateSourceTypesystemProperty() throws Exception {
        // Disabling Post processor
        Config.setParameter("migration.data.postprocessor.tscheck.disable", "true");

        if (BooleanUtils.isFalse(Config.getBoolean(MIGRATION_UPDATE_TYPE_SYSTEM, false))) {
            return;
        }
        DataRepository sourceRepository = migrationContext.getDataSourceRepository();
        try (Connection connection = sourceRepository.getConnection();
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(String.format(TYPESYSTEM_SELECT_STATEMENT,
                        sourceRepository.getDataSourceConfiguration().getSchema(), "CCV2_TYPESYSTEM_MIGRATIONS"))) {
            LOG.debug("SETTING the Type System from CCV2_TYPESYSTEM_MIGRATIONS"
                    + String.format(TYPESYSTEM_SELECT_STATEMENT,
                            sourceRepository.getDataSourceConfiguration().getSchema(), "CCV2_TYPESYSTEM_MIGRATIONS"));

            String typeSystemName;
            if (resultSet.next()) {
                typeSystemName = resultSet.getString("name");

                if (StringUtils.isNotEmpty(typeSystemName)) {
                    Config.setParameter(SOURCE_TYPESYSTEMNAME, typeSystemName);
                    LOG.info("SETTING typeSystemName = " + typeSystemName);
                }
            }
        }
    }

    protected void updateTypesystemTable(Set<String> migrationItems) throws Exception {

        if (BooleanUtils.isFalse(Config.getBoolean(MIGRATION_UPDATE_TYPE_SYSTEM, false))) {
            return;
        }
        DataRepository sourceRepository = migrationContext.getDataSourceRepository();
        for (final String tableName : migrationItems) {
            if (Arrays.stream(TYPE_SYSTEM_RELATED_TYPES)
                    .anyMatch(t -> StringUtils.startsWithIgnoreCase(tableName, t))) {
                try (Connection connection = sourceRepository.getConnection();
                        Statement stmt = connection.createStatement();
                        ResultSet resultSet = stmt.executeQuery(String.format(TYPESYSTEM_SELECT_STATEMENT,
                                sourceRepository.getDataSourceConfiguration().getSchema(),
                                "CCV2_TYPESYSTEM_MIGRATIONS"))) {
                    LOG.debug("Type System table - table found in list, get latest TS => " + String.format(
                            TYPESYSTEM_SELECT_STATEMENT, sourceRepository.getDataSourceConfiguration().getSchema(),
                            "CCV2_TYPESYSTEM_MIGRATIONS"));
                    String typeSystemName = null;
                    if (resultSet.next()) {
                        typeSystemName = resultSet.getString("name");
                    } else {
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
                    if (typeSystemtableresultSet.next()) {
                        typeSystemTableName = typeSystemtableresultSet.getString("TableName");
                    }

                    if (typeSystemTableName != null) {
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
    private String extractTSbaseTableName(final String tableNameFromMigrationItems) {
        String tsBaseTableName = tableNameFromMigrationItems;

        // if it ends with lp
        if (tableNameFromMigrationItems.toLowerCase().endsWith("lp")) {
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
            status = databaseMigrationService.getMigrationState(context, migrationID, sinceTime);
            Thread.sleep(5000);
            since = System.currentTimeMillis();
            if (isJobStateAborted(cronJobModel)) {
                aborted = true;
                break;
            }
        } while (StringUtils.equalsAnyIgnoreCase(status.getStatus().toString(), RUNNING_MIGRATION));

        if (aborted) {
            LOG.info(" Aborted ...STOPPING migration ");
            databaseMigrationService.stopMigration(migrationContext, currentMigrationId);
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

    protected LaunchOptions createLaunchOptions(MigrationCronJobModel migrationCronJob) {
        final LaunchOptions launchOptions = new LaunchOptions();

        putLaunchOptionProperty(launchOptions, CommercedbsyncConstants.MIGRATION_DATA_MAXPRALLELTABLECOPY,
                migrationCronJob.getMaxParallelTableCopy());
        putLaunchOptionProperty(launchOptions, CommercedbsyncConstants.MIGRATION_DATA_WORKERS_READER_MAXTASKS,
                migrationCronJob.getMaxReaderWorkers());
        putLaunchOptionProperty(launchOptions, CommercedbsyncConstants.MIGRATION_DATA_WORKERS_WRITER_MAXTASKS,
                migrationCronJob.getMaxWriterWorkers());
        putLaunchOptionProperty(launchOptions, CommercedbsyncConstants.MIGRATION_DATA_READER_BATCHSIZE,
                migrationCronJob.getBatchSize());

        return launchOptions;
    }

    private void putLaunchOptionProperty(final LaunchOptions launchOptions, String property, Serializable value) {
        launchOptions.getPropertyOverrideMap().put(property,
                Optional.ofNullable(value).orElseGet(() -> Config.getInt(property, 1)));
    }

    protected boolean isJobStateAborted(final CronJobModel cronJobModel) {
        this.modelService.refresh(cronJobModel);
        LOG.info("cron job status = " + cronJobModel.getStatus());
        LOG.info("cron job request to abort =" + cronJobModel.getRequestAbort());
        return ((cronJobModel.getStatus() == CronJobStatus.ABORTED)
                || (cronJobModel.getRequestAbort() != null && cronJobModel.getRequestAbort()));
    }

    @Override
    public boolean isAbortable() {
        return true;
    }

    public void setMigrationContext(MigrationContext migrationContext) {
        this.migrationContext = migrationContext;
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
