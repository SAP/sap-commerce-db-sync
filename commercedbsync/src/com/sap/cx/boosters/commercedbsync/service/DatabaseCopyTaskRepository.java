/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */
package com.sap.cx.boosters.commercedbsync.service;

import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.CopyContext.DataCopyItem;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTask;

import java.time.OffsetDateTime;
import java.util.Set;

/**
 * Repository to manage Migration Status and Tasks
 */
public interface DatabaseCopyTaskRepository {

    /**
     * Creates a new DB Migration status record
     *
     * @param context
     * @throws Exception
     */
    void createMigrationStatus(CopyContext context) throws Exception;

    /**
     * Updates the Migration status record
     *
     * @param context
     * @param progress
     * @throws Exception
     */
    void setMigrationStatus(CopyContext context, MigrationProgress progress) throws Exception;


    /**
     * Updates the Migration status record from one status to another
     *
     * @param context
     * @param from
     * @param to
     * @return true if migration status is updated, false otherwise
     * @throws Exception
     */
    boolean setMigrationStatus(CopyContext context, MigrationProgress from, MigrationProgress to) throws Exception;

    /**
     * Retrieves the current migration status
     *
     * @param context
     * @return
     * @throws Exception
     */
    MigrationStatus getMigrationStatus(CopyContext context) throws Exception;

    /**
     * Schedules a copy Task
     *
     * @param context        the migration context
     * @param copyItem       the item to copy
     * @param sourceRowCount
     * @param targetNode     the nodeId to perform the copy
     * @throws Exception
     */
    void scheduleTask(CopyContext context, CopyContext.DataCopyItem copyItem, long sourceRowCount, int targetNode) throws Exception;

    /**
     * Retrieves all pending tasks
     *
     * @param context
     * @return
     * @throws Exception
     */
    Set<DatabaseCopyTask> findPendingTasks(CopyContext context) throws Exception;

    /**
     * Updates progress on a Task
     *
     * @param context
     * @param copyItem
     * @param itemCount
     * @throws Exception
     */
    void updateTaskProgress(CopyContext context, CopyContext.DataCopyItem copyItem, long itemCount) throws Exception;

    /**
     * Marks the Task as Completed
     *
     * @param context
     * @param copyItem
     * @param duration
     * @throws Exception
     */
    void markTaskCompleted(CopyContext context, CopyContext.DataCopyItem copyItem, String duration) throws Exception;

    /**
     * Marks the Task as Failed
     *
     * @param context
     * @param copyItem
     * @param error
     * @throws Exception
     */
    void markTaskFailed(CopyContext context, CopyContext.DataCopyItem copyItem, Exception error) throws Exception;

    /**
     * Gets all updated Tasks
     *
     * @param context
     * @param since   offset
     * @return
     * @throws Exception
     */
    Set<DatabaseCopyTask> getUpdatedTasks(CopyContext context, OffsetDateTime since) throws Exception;

    Set<DatabaseCopyTask> getAllTasks(CopyContext context) throws Exception;
     /**
	 * ORACLE_TARGET -- added duration ins econds Marks the Task as Completed
	 *
	 * @param context
	 * @param copyItem
	 * @param duration
	 * @throws Exception
	 */
void markTaskCompleted(CopyContext context, DataCopyItem copyItem, String duration, float durationseconds)
			throws Exception;
}
