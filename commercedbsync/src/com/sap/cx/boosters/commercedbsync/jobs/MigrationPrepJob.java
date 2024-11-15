/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.jobs;

import com.sap.cx.boosters.commercedbsync.service.impl.DefaultDatabaseMigrationService;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationPrepJob extends AbstractMigrationJobPerformable {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseMigrationService.class);

    @Override
    public PerformResult perform(CronJobModel cronJobModel) {
        boolean caughtExeption = false;
        try {
            databaseMigrationService.prepareMigration(migrationContext);
        } catch (final Exception e) {
            caughtExeption = true;
            LOG.error(" Exception caught: message= " + e.getMessage(), e);
        }
        return new PerformResult(caughtExeption ? CronJobResult.FAILURE : CronJobResult.SUCCESS,
                CronJobStatus.FINISHED);
    }
}
