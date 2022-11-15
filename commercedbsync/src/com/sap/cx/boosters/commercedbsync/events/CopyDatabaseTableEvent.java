/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */
package com.sap.cx.boosters.commercedbsync.events;

/**
 * Cluster Event to notify a Cluster to start the copy process
 */
public class CopyDatabaseTableEvent extends CopyEvent {
    public CopyDatabaseTableEvent(final Integer sourceNodeId, final String migrationId) {
        super(sourceNodeId, migrationId);
    }
}
