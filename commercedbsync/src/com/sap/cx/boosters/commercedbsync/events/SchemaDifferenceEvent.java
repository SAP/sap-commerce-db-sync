/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events;

public class SchemaDifferenceEvent extends OperationEvent {

    public SchemaDifferenceEvent(final int sourceNodeId, final String migrationId, final boolean reversed) {
        super(sourceNodeId, migrationId, reversed);
    }

    public String getSchemaDifferenceId() {
        return getOperationId();
    }
}
