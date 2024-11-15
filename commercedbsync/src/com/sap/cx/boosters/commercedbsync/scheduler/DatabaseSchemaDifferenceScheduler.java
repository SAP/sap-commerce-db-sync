/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler;

import com.sap.cx.boosters.commercedbsync.context.SchemaDifferenceContext;

/**
 * Scheduler for Schema Difference logic execution
 */
public interface DatabaseSchemaDifferenceScheduler {

    void schedule(SchemaDifferenceContext context) throws Exception;

    void abort(SchemaDifferenceContext context) throws Exception;

}
