/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events;

/**
 * Event to signal completion of the assigned copy tasks and willing to accept
 * next work batch
 */
public class NodeAvailabilityEvent extends OperationEvent {
    public NodeAvailabilityEvent(final Integer sourceNodeId, final String migrationId, final boolean reversed) {
        super(sourceNodeId, migrationId, reversed);
    }
}
