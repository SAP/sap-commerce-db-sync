/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler;

import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;

import java.time.OffsetDateTime;

/**
 * Scheduler for Cluster Migration
 */
public interface DatabaseCopyScheduler {
    void schedule(CopyContext context) throws Exception;

    void resumeUnfinishedItems(CopyContext copyContext) throws Exception;

    MigrationStatus getCurrentState(CopyContext context, OffsetDateTime since) throws Exception;

    boolean isAborted(CopyContext context) throws Exception;

    void abort(CopyContext context) throws Exception;
}
