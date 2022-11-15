/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */
package com.sap.cx.boosters.commercedbsync.jobs;

import com.google.common.base.Preconditions;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.jalo.AbortCronJobException;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.model.cron.FullMigrationCronJobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;


/**
 * This class offers functionality for FullMigrationJob.
 */
public class FullMigrationJob extends AbstractMigrationJobPerformable {

  private static final Logger LOG = LoggerFactory.getLogger(FullMigrationJob.class);

  @Override
  public PerformResult perform(final CronJobModel cronJobModel) {
    FullMigrationCronJobModel fullMigrationCronJobModel;

    Preconditions
        .checkState((cronJobModel instanceof FullMigrationCronJobModel),
            "cronJobModel must the instance of FullMigrationCronJobModel");
    fullMigrationCronJobModel = (FullMigrationCronJobModel) cronJobModel;
    Preconditions.checkNotNull(fullMigrationCronJobModel.getMigrationItems(),
        "We expect at least one table for the full migration");
    Preconditions.checkState(
        null != fullMigrationCronJobModel.getMigrationItems() && !fullMigrationCronJobModel
            .getMigrationItems().isEmpty(),
        "We expect at least one table for the full migration");

    boolean caughtExeption = false;
    try {
        incrementalMigrationContext
            .setIncludedTables(fullMigrationCronJobModel.getMigrationItems());
			// ORACLE_TARGET - START there is scope to make the 2 update methods
			// efficient
			updateSourceTypesystemProperty();
			// ORACLE_TARGET - END there is scope to make the 2 methods
			// efficient
         updateTypesystemTable(fullMigrationCronJobModel.getMigrationItems());
         incrementalMigrationContext.setDeletionEnabled(false);
         incrementalMigrationContext.setLpTableMigrationEnabled(false);
		 incrementalMigrationContext.setTruncateEnabled(fullMigrationCronJobModel.isTruncateEnabled());
         incrementalMigrationContext.setSchemaMigrationAutoTriggerEnabled(fullMigrationCronJobModel.isSchemaAutotrigger());
         incrementalMigrationContext.setIncrementalModeEnabled(false);
         currentMigrationId = databaseMigrationService.startMigration(incrementalMigrationContext);
		 MigrationStatus currentState = waitForFinishCronjobs(incrementalMigrationContext, currentMigrationId,cronJobModel);
		}
	    catch (final AbortCronJobException e)
		{
			return new PerformResult(CronJobResult.ERROR, CronJobStatus.ABORTED);
		}
		catch (final Exception e)
		{
			caughtExeption = true;
			LOG.error(" Exception caught: message= " + e.getMessage(), e);
		}
		return new PerformResult(caughtExeption ? CronJobResult.FAILURE : CronJobResult.SUCCESS, CronJobStatus.FINISHED);
	  }

}
