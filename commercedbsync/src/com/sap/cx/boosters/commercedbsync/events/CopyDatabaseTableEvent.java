/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events;

import java.io.Serializable;
import java.util.Map;

/**
 * Cluster Event to notify a Cluster to start the copy process
 */
public class CopyDatabaseTableEvent extends OperationEvent {

    /**
     * contains property value updates that should be populated in the cluster
     */
    private final Map<String, Serializable> propertyOverrideMap;

    public CopyDatabaseTableEvent(final Integer sourceNodeId, final String migrationId,
            final Map<String, Serializable> propertyOverrideMap, final boolean reversed) {
        super(sourceNodeId, migrationId, reversed);
        this.propertyOverrideMap = propertyOverrideMap;
    }

    public Map<String, Serializable> getPropertyOverrideMap() {
        return propertyOverrideMap;
    }
}
