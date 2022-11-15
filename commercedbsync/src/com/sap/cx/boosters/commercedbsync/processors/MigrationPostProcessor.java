/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;

/**
 * Postprocessor activated after a migration has terminated
 */
public interface MigrationPostProcessor {

    void process(CopyContext context);
}
