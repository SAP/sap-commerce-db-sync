/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events;

/**
 * * ClusterAwareEvent to signal completion of the assigned copy ta
 */
public class CopyCompleteEvent extends OperationEvent {
    private final Boolean copyResult = false;

    public CopyCompleteEvent(final Integer sourceNodeId, final String migrationId, final boolean reversed) {
        super(sourceNodeId, migrationId, reversed);
    }

    public Boolean getCopyResult() {
        return copyResult;
    }
}
