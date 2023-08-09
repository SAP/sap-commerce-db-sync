/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;

/**
 * Preproscessor activated before a migration starts
 */
public interface MigrationPreProcessor {

    void process(CopyContext context);

    default boolean shouldExecute(CopyContext context) {
        return true;
    }
}
