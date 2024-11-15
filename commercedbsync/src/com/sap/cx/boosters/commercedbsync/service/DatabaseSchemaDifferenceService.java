/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service;

import com.sap.cx.boosters.commercedbsync.SchemaDifferenceStatus;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.context.SchemaDifferenceContext;
import com.sap.cx.boosters.commercedbsync.service.impl.DefaultDatabaseSchemaDifferenceService;

import java.util.concurrent.CompletableFuture;

/**
 * Calculates and applies Schema Differences between two Databases
 */
public interface DatabaseSchemaDifferenceService {

    String generateSchemaDifferencesSql(MigrationContext context,
            DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult differenceResult) throws Exception;

    void executeSchemaDifferencesSql(MigrationContext context, String sql) throws Exception;

    void executeSchemaDifferences(MigrationContext context) throws Exception;

    /**
     * Calculates the differences between two schemas
     *
     * @param migrationContext
     * @return
     */
    DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult getSchemaDifferenceFromStatus(
            MigrationContext migrationContext, SchemaDifferenceStatus schemaDifferenceStatus) throws Exception;

    DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult createSchemaDifferenceResult(
            MigrationContext migrationContext) throws Exception;

    SchemaDifferenceStatus getSchemaDifferenceStatusById(String schemaDifferenceId, MigrationContext migrationContext)
            throws Exception;

    SchemaDifferenceStatus getMostRecentSchemaDifference(MigrationContext migrationContext) throws Exception;

    CompletableFuture<Void> checkSchemaDifferenceAsync(SchemaDifferenceContext context);

    void abortRunningSchemaDifference(MigrationContext migrationContext) throws Exception;

    String startSchemaDifferenceCheck(MigrationContext context) throws Exception;

    SchemaDifferenceStatus startSchemaDifferenceCheckAndWaitForFinish(MigrationContext context) throws Exception;
}
