/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service;

import com.sap.cx.boosters.commercedbsync.service.impl.DefaultDatabaseSchemaDifferenceService;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

/**
 * Calculates and applies Schema Differences between two Databases
 */
public interface DatabaseSchemaDifferenceService {

    String generateSchemaDifferencesSql(MigrationContext context) throws Exception;

    void executeSchemaDifferencesSql(MigrationContext context, String sql) throws Exception;

    void executeSchemaDifferences(MigrationContext context) throws Exception;

    /**
     * Calculates the differences between two schemas
     *
     * @param migrationContext
     * @return
     */
    DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult getDifference(MigrationContext migrationContext)
            throws Exception;
}
