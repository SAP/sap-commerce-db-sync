/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service;

import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.CopyContext.DataCopyItem;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Repository to manage Migration Status and Tasks
 */
public interface DatabaseCopyTaskRepository {
    /**
     * Get migration ID of most recent migration found
     *
     * @param context
     * @return migration ID
     */
    String getMostRecentMigrationID(MigrationContext context);

    /**
     * Creates a new DB Migration status record
     *
     * @param context
     * @throws Exception
     */
    void createMigrationStatus(CopyContext context, int numberOfItems) throws Exception;

    /**
     * Resets the values of the current migration to start it again
     *
     * @param context
     * @throws Exception
     */
    void resetMigration(CopyContext context) throws Exception;

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
     * Retrieves the currently running migration status
     *
     * @return status in case of running migration, null otherwise
     * @throws Exception
     */
    MigrationStatus getRunningMigrationStatus(MigrationContext context);

    /**
     * Schedules a copy Task
     *
     * @param context
     *            the migration context
     * @param copyItem
     *            the item to copy
     * @param sourceRowCount
     * @param targetNode
     *            the nodeId to perform the copy
     * @throws Exception
     */
    void scheduleTask(CopyContext context, CopyContext.DataCopyItem copyItem, long itemOrder, long sourceRowCount,
            int targetNode) throws Exception;

    void rescheduleTask(CopyContext context, String pipelineName, int targetNodeId) throws Exception;

    void scheduleBatch(CopyContext context, CopyContext.DataCopyItem copyItem, int batchId, Object lowerBoundary,
            Object upperBoundary) throws Exception;

    void scheduleBatch(CopyContext context, DataCopyItem copyItem, int batchId, Object lowerBoundary,
            Object upperBoundary, String partition) throws Exception;

    void markBatchCompleted(CopyContext context, DataCopyItem copyItem, int batchId) throws Exception;

    void markBatchCompleted(CopyContext context, DataCopyItem copyItem, int batchId, String partition) throws Exception;

    void resetPipelineBatches(CopyContext context, CopyContext.DataCopyItem copyItem) throws Exception;

    void resetPipelineBatches(CopyContext context, DataCopyItem copyItem, String partition) throws Exception;

    Set<DatabaseCopyBatch> findPendingBatchesForPipeline(CopyContext context, CopyContext.DataCopyItem item)
            throws Exception;

    Set<DatabaseCopyBatch> findPendingBatchesForPipeline(CopyContext context, DataCopyItem item, String partition)
            throws Exception;

    Optional<DatabaseCopyTask> findPipeline(CopyContext context, CopyContext.DataCopyItem dataCopyItem)
            throws Exception;

    boolean findInAllPipelines(CopyContext context, CopyContext.DataCopyItem dataCopyItem,
            Predicate<DatabaseCopyTask> p) throws Exception;

    /**
     * Retrieves all pending tasks
     *
     * @param context
     * @return
     * @throws Exception
     */
    Set<DatabaseCopyTask> findPendingTasks(CopyContext context) throws Exception;

    Set<DatabaseCopyTask> findFailedTasks(CopyContext context) throws Exception;

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
     * Mark all remaining table copy tasks as aborted, should be used when migration
     * was stopped unexpectedly (for example after application crashed)
     *
     * @param context
     * @throws Exception
     */
    void markRemainingTasksAborted(CopyContext context) throws Exception;

    void markTaskTruncated(CopyContext context, CopyContext.DataCopyItem copyItem) throws Exception;

    /**
     * Marks the Task as Failed
     *
     * @param context
     * @param copyItem
     * @param error
     * @throws Exception
     */
    void markTaskFailed(CopyContext context, CopyContext.DataCopyItem copyItem, Exception error) throws Exception;

    void updateTaskCopyMethod(CopyContext context, CopyContext.DataCopyItem copyItem, String copyMethod)
            throws Exception;

    void updateTaskKeyColumns(CopyContext context, CopyContext.DataCopyItem copyItem, Collection<String> keyColumns)
            throws Exception;

    /**
     * Gets all updated Tasks
     *
     * @param context
     * @param since
     *            offset
     * @return
     * @throws Exception
     */
    Set<DatabaseCopyTask> getUpdatedTasks(CopyContext context, OffsetDateTime since) throws Exception;

    Set<DatabaseCopyTask> getAllTasks(CopyContext context) throws Exception;

    /**
     * ORACLE_TARGET -- added duration in seconds Marks the Task as Completed
     *
     * @param context
     * @param copyItem
     * @param duration
     * @throws Exception
     */
    void markTaskCompleted(CopyContext context, DataCopyItem copyItem, String duration, float durationseconds)
            throws Exception;
}
