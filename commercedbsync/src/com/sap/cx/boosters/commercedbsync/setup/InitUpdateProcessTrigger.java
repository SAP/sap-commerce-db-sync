/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.setup;

import com.sap.cx.boosters.commercedbsync.context.LaunchOptions;
import de.hybris.platform.media.services.MediaStorageInitializer;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitUpdateProcessTrigger implements MediaStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(InitUpdateProcessTrigger.class);

    private final MigrationContext migrationContext;
    private final DatabaseMigrationService databaseMigrationService;
    private boolean failOnError = false;

    public InitUpdateProcessTrigger(MigrationContext migrationContext,
            DatabaseMigrationService databaseMigrationService) {
        this.migrationContext = migrationContext;
        this.databaseMigrationService = databaseMigrationService;
    }

    @Override
    public void onInitialize() {
        // Do nothing
    }

    @Override
    public void onUpdate() {
        try {
            if (migrationContext.isMigrationTriggeredByUpdateProcess()) {
                LOG.info("Starting data migration ...");
                String migrationId = databaseMigrationService.startMigration(migrationContext, LaunchOptions.NONE);
                databaseMigrationService.waitForFinish(migrationContext, migrationId);
                // note: further update activities not stopped here -> should we?
            }
        } catch (Exception e) {
            failOnError = migrationContext.isFailOnErrorEnabled();
            if (failOnError) {
                throw new Error(e);
            }
        }
    }

    @Override
    public boolean failOnInitUpdateError() {
        return failOnError;
    }

}
