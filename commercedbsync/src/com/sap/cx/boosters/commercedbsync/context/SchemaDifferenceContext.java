/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context;

import java.util.UUID;

/**
 * Contains the Information needed to perform Schema Differences Check
 */
public class SchemaDifferenceContext {

    private final String schemaDifferenceId;

    private final MigrationContext migrationContext;

    public SchemaDifferenceContext(MigrationContext migrationContext) {
        this.schemaDifferenceId = UUID.randomUUID().toString();
        this.migrationContext = migrationContext;
    }

    public SchemaDifferenceContext(String schemaDifferenceId, MigrationContext migrationContext) {
        this.schemaDifferenceId = schemaDifferenceId;
        this.migrationContext = migrationContext;
    }

    public String getSchemaDifferenceId() {
        return schemaDifferenceId;
    }

    public MigrationContext getMigrationContext() {
        return migrationContext;
    }
}
