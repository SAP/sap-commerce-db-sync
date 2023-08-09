/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.jobs;

import com.google.common.base.Preconditions;
import com.sap.cx.boosters.commercedbsync.context.IncrementalMigrationContext;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.jalo.AbortCronJobException;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import com.sap.cx.boosters.commercedbsync.model.cron.FullMigrationCronJobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class offers functionality for FullMigrationJob.
 */
public class FullMigrationJob extends AbstractMigrationJobPerformable {

    private static final Logger LOG = LoggerFactory.getLogger(FullMigrationJob.class);

    @Override
    public PerformResult perform(final CronJobModel cronJobModel) {
        FullMigrationCronJobModel fullMigrationCronJobModel;

        Preconditions.checkState(migrationContext instanceof IncrementalMigrationContext,
                "Migration context is not activated for data export via cron job");
        Preconditions.checkState((cronJobModel instanceof FullMigrationCronJobModel),
                "cronJobModel must the instance of FullMigrationCronJobModel");
        fullMigrationCronJobModel = (FullMigrationCronJobModel) cronJobModel;
        Preconditions.checkNotNull(fullMigrationCronJobModel.getMigrationItems(),
                "We expect at least one table for the full migration");
        Preconditions.checkState(
                null != fullMigrationCronJobModel.getMigrationItems()
                        && !fullMigrationCronJobModel.getMigrationItems().isEmpty(),
                "We expect at least one table for the full migration");

        final IncrementalMigrationContext incrementalMigrationContext = (IncrementalMigrationContext) migrationContext;
        boolean caughtExeption = false;
        try {
            incrementalMigrationContext.setIncludedTables(fullMigrationCronJobModel.getMigrationItems());
            updateSourceTypesystemProperty();
            updateTypesystemTable(fullMigrationCronJobModel.getMigrationItems());
            incrementalMigrationContext.setDeletionEnabled(false);
            incrementalMigrationContext.setLpTableMigrationEnabled(false);
            incrementalMigrationContext.setTruncateEnabled(fullMigrationCronJobModel.isTruncateEnabled());
            incrementalMigrationContext
                    .setSchemaMigrationAutoTriggerEnabled(fullMigrationCronJobModel.isSchemaAutotrigger());
            incrementalMigrationContext.setIncrementalModeEnabled(false);
            incrementalMigrationContext
                    .setFullDatabaseMigrationEnabled(fullMigrationCronJobModel.isFullDatabaseMigration());
            currentMigrationId = databaseMigrationService.startMigration(incrementalMigrationContext,
                    createLaunchOptions(fullMigrationCronJobModel));

            waitForFinishCronjobs(incrementalMigrationContext, currentMigrationId, cronJobModel);
        } catch (final AbortCronJobException e) {
            return new PerformResult(CronJobResult.ERROR, CronJobStatus.ABORTED);
        } catch (final Exception e) {
            caughtExeption = true;
            LOG.error(" Exception caught: message= " + e.getMessage(), e);
        }
        return new PerformResult(caughtExeption ? CronJobResult.FAILURE : CronJobResult.SUCCESS,
                CronJobStatus.FINISHED);
    }

}
