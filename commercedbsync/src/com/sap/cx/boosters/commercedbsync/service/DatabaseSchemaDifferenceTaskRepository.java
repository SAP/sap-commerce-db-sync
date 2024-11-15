/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service;

import com.sap.cx.boosters.commercedbsync.SchemaDifferenceProgress;
import com.sap.cx.boosters.commercedbsync.SchemaDifferenceStatus;
import com.sap.cx.boosters.commercedbsync.context.SchemaDifferenceContext;
import com.sap.cx.boosters.commercedbsync.service.impl.DefaultDatabaseSchemaDifferenceService;
import de.hybris.platform.commercedbsynchac.data.SchemaDifferenceResultData;

/**
 * Repository to manage Schema Difference Status
 */
public interface DatabaseSchemaDifferenceTaskRepository {

    String MISSING_COLUMN_DELIMITER = ";";

    /**
     * Get ID of most recent Schema Difference Status found
     *
     * @param context
     *            the schema difference context
     * @return Schema Difference ID
     */
    String getMostRecentSchemaDifferenceId(SchemaDifferenceContext context);

    /**
     * Creates a new Schema Difference Status record
     *
     * @param context
     *            the schema difference context
     * @throws Exception
     */
    void createSchemaDifferenceStatus(SchemaDifferenceContext context) throws Exception;

    /**
     * Updates the Schema Difference Status record
     *
     * @param context
     * @param progress
     * @throws Exception
     */
    void setSchemaDifferenceStatus(SchemaDifferenceContext context, SchemaDifferenceProgress progress) throws Exception;

    /**
     * Retrieves the Schema Difference Status
     *
     * @param context
     *            the schema difference context
     * @return
     * @throws Exception
     */
    SchemaDifferenceStatus getSchemaDifferenceStatus(SchemaDifferenceContext context) throws Exception;

    /**
     * Retrieves the Schema Difference Result Data
     *
     * @param context
     *            the schema difference context
     * @param referenceDatabase
     *            the reference database (source/target)
     * @return
     * @throws Exception
     */
    SchemaDifferenceResultData getSchemaDifferenceResultData(SchemaDifferenceContext context, String referenceDatabase)
            throws Exception;

    /**
     * Creates a new Schema Difference Result record
     *
     * @param context
     *            the schema difference context
     * @param referenceDatabase
     *            the reference database (source/target)
     * @throws Exception
     */
    void saveSchemaDifference(SchemaDifferenceContext context,
            DefaultDatabaseSchemaDifferenceService.SchemaDifference schemaDifference, String referenceDatabase)
            throws Exception;

    /**
     * Updates SQL Script in Schema Difference Status record
     *
     * @param context
     *            the schema difference context
     * @param sqlScript
     *            the SQL script required for adjusting schema changes
     * @throws Exception
     */
    void saveSqlScript(SchemaDifferenceContext context, String sqlScript) throws Exception;

    /**
     * Schedules a Schema Difference Task
     *
     * @param context
     *            the schema difference context
     * @param pipelinename
     *            the pipeline name
     * @throws Exception
     */
    void scheduleTask(SchemaDifferenceContext context, String pipelinename) throws Exception;

    /**
     * Marks the Task as Completed
     *
     * @param context
     *            the schema difference context
     * @throws Exception
     */
    void markTaskCompleted(SchemaDifferenceContext context, String pipelinename, String duration, float durationseconds)
            throws Exception;

    /**
     * Marks the Task as Failed
     *
     * @param context
     *            the schema difference context
     * @throws Exception
     */
    void markTaskFailed(SchemaDifferenceContext context, String pipelinename, Exception error) throws Exception;
}
