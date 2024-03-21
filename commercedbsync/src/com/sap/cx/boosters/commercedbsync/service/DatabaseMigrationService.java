/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service;

import com.sap.cx.boosters.commercedbsync.MigrationReport;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.context.LaunchOptions;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

import java.time.OffsetDateTime;

/**
 * Entry point to trigger a migration process and track the status
 */
public interface DatabaseMigrationService {

    /**
     * Asynchronously start a new database migration
     *
     * @param context
     *            Migration configuration
     * @param launchOptions
     * @return migrationID of the started migration
     * @throws Exception
     *             if anything goes wrong during start
     */
    String startMigration(MigrationContext context, LaunchOptions launchOptions) throws Exception;

    /**
     * Asynchronously resumes the migrations and continuous with unfinished items.
     *
     * @param context
     *            Migration configuration
     * @param launchOptions
     * @param migrationID
     *            The id of the migration to resume
     * @throws Exception
     *             if anything goes wrong during start
     */
    void resumeUnfinishedMigration(MigrationContext context, LaunchOptions launchOptions, String migrationID)
            throws Exception; // TODO use this from hac/job

    /**
     * Stops the the database migration process. The process is stopped on all
     * nodes, in case clustering is used.
     *
     * @param context
     *            Migration configuration
     * @param migrationID
     *            ID of the migration process that should be stopped
     * @throws Exception
     *             if anything goes wrong
     */
    void stopMigration(MigrationContext context, String migrationID) throws Exception;

    /**
     * Mark all remaining table copy tasks as aborted, should be used when migration
     * was stopped unexpectedly (for example after application crashed)
     *
     * @param context
     *            Migration configuration
     * @param migrationID
     *            ID of the migration process tasks that should be marked as aborted
     * @throws Exception
     */
    void markRemainingTasksAborted(MigrationContext context, String migrationID) throws Exception;

    /**
     * Get current overall state without details
     *
     * @param context
     * @param migrationID
     * @return
     * @throws Exception
     */
    MigrationStatus getMigrationState(MigrationContext context, String migrationID) throws Exception;

    /**
     * Get current state with details per copy task
     *
     * @param context
     * @param migrationID
     * @param since
     *            Get all updates since this timestamp. Must be in UTC!
     * @return
     * @throws Exception
     */
    MigrationStatus getMigrationState(MigrationContext context, String migrationID, OffsetDateTime since)
            throws Exception;

    MigrationReport getMigrationReport(MigrationContext context, String migrationID) throws Exception;

    String getMigrationID(MigrationContext migrationContext);

    /**
     * Busy wait until migration is done. Use only for tests!
     *
     * @param context
     * @param migrationID
     * @return
     * @throws Exception
     *             when migration was not successful
     */
    MigrationStatus waitForFinish(MigrationContext context, String migrationID) throws Exception;
}
