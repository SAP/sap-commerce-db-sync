/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events;

/**
 * * ClusterAwareEvent to signal completion of the assigned copy ta
 */
public class CopyCompleteEvent extends OperationEvent {
    private final boolean copyResult;

    public CopyCompleteEvent(final Integer sourceNodeId, final String migrationId, final boolean reversed,
            final boolean copyResult) {
        super(sourceNodeId, migrationId, reversed);
        this.copyResult = copyResult;
    }

    public Boolean getCopyResult() {
        return copyResult;
    }
}
