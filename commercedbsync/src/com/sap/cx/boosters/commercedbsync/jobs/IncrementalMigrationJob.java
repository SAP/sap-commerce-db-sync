/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.jobs;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.IncrementalMigrationContext;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.jalo.AbortCronJobException;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.jalo.type.TypeManager;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import de.hybris.platform.servicelayer.type.TypeService;
import de.hybris.platform.util.Config;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.model.cron.IncrementalMigrationCronJobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class offers functionality for IncrementalMigrationJob.
 */
public class IncrementalMigrationJob extends AbstractMigrationJobPerformable {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalMigrationJob.class);

    private static final String LP_SUFFIX = "lp";

    private static final String TABLE_PREFIX = Config.getString("db.tableprefix", "");

    private static final boolean DELETIONS_BY_TYPECODES_ENABLED = Config
            .getBoolean(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_TYPECODES_ENABLED, false);
    private static final boolean DELETIONS_BY_ITEMTYPES_ENABLED = Config
            .getBoolean(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_ITEMTYPES_ENABLED, false);

    private static final String TABLE_EXISTS_SELECT_STATEMENT_MSSQL = "SELECT TABLE_NAME \n"
            + "  FROM INFORMATION_SCHEMA.TABLES \n" + "  WHERE TABLE_SCHEMA = '%s' \n" + "  AND TABLE_NAME = '%2$s'\n";
    private static final String TABLE_EXISTS_SELECT_STATEMENT_ORACLE = "SELECT TABLE_NAME \n" + " FROM dba_tables \n"
            + " WHERE upper(owner) = upper('%s') \n" + " AND   upper(table_name) = upper('%2$s') ";

    private static final String TABLE_EXISTS_SELECT_STATEMENT_HANA = "SELECT TABLE_NAME \n" + " FROM public.tables \n"
            + " WHERE schema_name = upper('%s') \n" + " AND   table_name = upper('%2$s') ";

    private static final String TABLE_EXISTS_SELECT_STATEMENT_POSTGRES = "SELECT TABLE_NAME \n"
            + " FROM public.tables \n" + " WHERE schema_name = upper('%s') \n" + " AND   table_name = upper('%2$s') ";

    @Resource(name = "typeService")
    private TypeService typeService;

    @Override
    public PerformResult perform(final CronJobModel cronJobModel) {
        IncrementalMigrationCronJobModel incrementalMigrationCronJob;

        Preconditions.checkState(migrationContext instanceof IncrementalMigrationContext,
                "Migration context is not activated for data export via cron job");
        Preconditions.checkState((cronJobModel instanceof IncrementalMigrationCronJobModel),
                "cronJobModel must the instance of FullMigrationCronJobModel");
        modelService.refresh(cronJobModel);

        incrementalMigrationCronJob = (IncrementalMigrationCronJobModel) cronJobModel;
        Preconditions.checkState(
                null != incrementalMigrationCronJob.getMigrationItems()
                        && !incrementalMigrationCronJob.getMigrationItems().isEmpty(),
                "We expect at least one table for the incremental migration");
        final Set<String> deletionTableSet = getDeletionTableSet(incrementalMigrationCronJob.getMigrationItems());
        final IncrementalMigrationContext incrementalMigrationContext = (IncrementalMigrationContext) migrationContext;
        MigrationStatus currentState;
        String currentMigrationId;
        boolean caughtExeption = false;
        try {

            if (null != incrementalMigrationCronJob.getLastStartTime()) {
                Instant timeStampInstant = incrementalMigrationCronJob.getLastStartTime().toInstant();
                LOG.info("For {} IncrementalTimestamp : {}  ", incrementalMigrationCronJob.getCode(), timeStampInstant);
                incrementalMigrationContext.setIncrementalMigrationTimestamp(timeStampInstant);
            } else {
                LOG.error(
                        "IncrementalTimestamp is not set for Cronjobs : {} , Aborting the migration, and please set the *lastStartTime* before triggering"
                                + " ",
                        incrementalMigrationCronJob.getCode());
                return new PerformResult(CronJobResult.ERROR, CronJobStatus.ABORTED);
            }
            incrementalMigrationContext.setIncrementalModeEnabled(true);
            incrementalMigrationContext
                    .setTruncateEnabled(Optional.ofNullable(incrementalMigrationCronJob.isTruncateEnabled())
                            .map(e -> incrementalMigrationCronJob.isTruncateEnabled()).orElse(false));
            updateSourceTypesystemProperty();
            if (CollectionUtils.isNotEmpty(deletionTableSet) && !isSchemaMigrationRequired(deletionTableSet)) {
                // deletionTableSet.add(deletionTable);
                LOG.info("Running Deletion incremental migration");
                incrementalMigrationContext.setSchemaMigrationAutoTriggerEnabled(false);
                incrementalMigrationContext.setIncrementalTables(deletionTableSet);
                incrementalMigrationContext.setDeletionEnabled(true);
                incrementalMigrationContext.setLpTableMigrationEnabled(false);
                currentMigrationId = databaseMigrationService.startMigration(incrementalMigrationContext,
                        createLaunchOptions(incrementalMigrationCronJob));

                databaseMigrationService.waitForFinish(migrationContext, currentMigrationId);
            }

            // Running incremental migration
            Set<String> tablesWithoutLp = incrementalMigrationCronJob.getMigrationItems().stream()
                    .filter(table -> !(StringUtils.endsWithIgnoreCase(table, LP_SUFFIX))).collect(Collectors.toSet());
            if (CollectionUtils.isNotEmpty(tablesWithoutLp)) {
                LOG.info("Running incremental migration for Non LP Table");
                incrementalMigrationContext.setDeletionEnabled(false);
                incrementalMigrationContext.setLpTableMigrationEnabled(false);
                incrementalMigrationContext.setIncrementalTables(tablesWithoutLp);
                incrementalMigrationContext
                        .setSchemaMigrationAutoTriggerEnabled(incrementalMigrationCronJob.isSchemaAutotrigger());
                currentMigrationId = databaseMigrationService.startMigration(incrementalMigrationContext,
                        createLaunchOptions(incrementalMigrationCronJob));

                waitForFinishCronjobs(incrementalMigrationContext, currentMigrationId, cronJobModel);
            }
            // Running incremental migration for LP Table
            Set<String> tablesWithLp = incrementalMigrationCronJob.getMigrationItems().stream()
                    .filter(table -> StringUtils.endsWithIgnoreCase(table, LP_SUFFIX)).collect(Collectors.toSet());
            if (CollectionUtils.isNotEmpty(tablesWithLp)) {
                LOG.info("Running incremental migration for LP Table");
                incrementalMigrationContext.setDeletionEnabled(false);
                incrementalMigrationContext.setLpTableMigrationEnabled(true);
                incrementalMigrationContext.setIncrementalTables(tablesWithLp);
                incrementalMigrationContext
                        .setSchemaMigrationAutoTriggerEnabled(incrementalMigrationCronJob.isSchemaAutotrigger());
                currentMigrationId = databaseMigrationService.startMigration(incrementalMigrationContext,
                        createLaunchOptions(incrementalMigrationCronJob));

                waitForFinishCronjobs(incrementalMigrationContext, currentMigrationId, cronJobModel);
            }
        } catch (final AbortCronJobException e) {
            return new PerformResult(CronJobResult.ERROR, CronJobStatus.ABORTED);
        } catch (final Exception e) {
            caughtExeption = true;
            LOG.error("Exception caught:", e);
        }
        if (!caughtExeption) {
            incrementalMigrationCronJob.setLastStartTime(cronJobModel.getStartTime());
            modelService.save(cronJobModel);
        }
        return new PerformResult(caughtExeption ? CronJobResult.FAILURE : CronJobResult.SUCCESS,
                CronJobStatus.FINISHED);
    }

    private Set<String> getDeletionTableSetFromItemType(Set<String> incMigrationItems) {
        String deletionItemTypes = Config
                .getString(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_ITEMTYPES, "");
        if (StringUtils.isEmpty(deletionItemTypes)) {
            return Collections.emptySet();
        }

        final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        final List<String> itemtypesArray = Splitter.on(",").omitEmptyStrings().trimResults()
                .splitToList(deletionItemTypes.toLowerCase());

        String tableName;
        for (String itemType : itemtypesArray) {
            tableName = typeService.getComposedTypeForCode(itemType).getTable();

            if (StringUtils.isNotEmpty(TABLE_PREFIX) && StringUtils.startsWith(tableName, TABLE_PREFIX)) {
                tableName = StringUtils.removeStart(tableName, TABLE_PREFIX);
            }
            if (incMigrationItems.contains(tableName)) {
                result.add(tableName);
            }
        }
        return result;
    }

    private Set<String> getDeletionTableSetFromTypeCodes(Set<String> incMigrationItems) {
        String deletionTypecodes = Config
                .getString(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_TYPECODES, "");
        if (StringUtils.isEmpty(deletionTypecodes)) {
            return Collections.emptySet();
        }

        final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        final List<String> typecodeArray = Splitter.on(",").omitEmptyStrings().trimResults()
                .splitToList(deletionTypecodes.toLowerCase());

        String tableName;
        for (String typecode : typecodeArray) {
            tableName = TypeManager.getInstance().getRootComposedType(Integer.valueOf(typecode)).getTable();

            if (StringUtils.startsWith(tableName, TABLE_PREFIX)) {
                tableName = StringUtils.removeStart(tableName, TABLE_PREFIX);
            }
            if (incMigrationItems.contains(tableName)) {
                result.add(tableName);
            }
        }
        return result;
    }

    private Set<String> getDeletionTableSet(Set<String> incMigrationItems) {
        Set<String> deletionTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        if (DELETIONS_BY_TYPECODES_ENABLED) {
            deletionTables.addAll(getDeletionTableSetFromTypeCodes(incMigrationItems));
        }

        if (DELETIONS_BY_ITEMTYPES_ENABLED) {
            deletionTables.addAll(getDeletionTableSetFromItemType(incMigrationItems));
        }

        return Collections.unmodifiableSet(deletionTables);
    }

    private boolean isSchemaMigrationRequired(Set<String> deletionTableSet) throws Exception {
        String TABLE_EXISTS_SELECT_STATEMENT;
        if (migrationContext.getDataTargetRepository().getDatabaseProvider().isHanaUsed()) {
            TABLE_EXISTS_SELECT_STATEMENT = TABLE_EXISTS_SELECT_STATEMENT_HANA;
        } else if (migrationContext.getDataTargetRepository().getDatabaseProvider().isOracleUsed()) {
            TABLE_EXISTS_SELECT_STATEMENT = TABLE_EXISTS_SELECT_STATEMENT_ORACLE;
        } else if (migrationContext.getDataTargetRepository().getDatabaseProvider().isMssqlUsed()) {
            TABLE_EXISTS_SELECT_STATEMENT = TABLE_EXISTS_SELECT_STATEMENT_MSSQL;
        } else if (migrationContext.getDataTargetRepository().getDatabaseProvider().isPostgreSqlUsed()) {
            TABLE_EXISTS_SELECT_STATEMENT = TABLE_EXISTS_SELECT_STATEMENT_POSTGRES;
        } else {
            TABLE_EXISTS_SELECT_STATEMENT = TABLE_EXISTS_SELECT_STATEMENT_MSSQL;
        }
        try (Connection connection = migrationContext.getDataTargetRepository().getConnection();
                Statement stmt = connection.createStatement()) {
            for (final String tableName : deletionTableSet) {
                try (ResultSet resultSet = stmt.executeQuery(String.format(TABLE_EXISTS_SELECT_STATEMENT,
                        migrationContext.getDataTargetRepository().getDataSourceConfiguration().getSchema(),
                        tableName))) {
                    String TABLE_NAME = null;
                    if (resultSet.next()) {
                        // TABLE_NAME = resultSet.getString("TABLE_NAME");
                    } else {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
