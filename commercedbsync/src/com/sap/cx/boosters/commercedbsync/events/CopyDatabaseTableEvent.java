/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events;

import java.io.Serializable;
import java.util.Map;

import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.UNUSED_CLUSTER_ID;

/**
 * Cluster Event to notify a Cluster to start the copy process
 */
public class CopyDatabaseTableEvent extends OperationEvent {

    /**
     * contains property value updates that should be populated in the cluster
     */
    private final Map<String, Serializable> propertyOverrideMap;
    private final int targetNodeId;

    public CopyDatabaseTableEvent(final Integer sourceNodeId, int targetNodeId, final String migrationId,
            final Map<String, Serializable> propertyOverrideMap, final boolean reversed) {
        super(sourceNodeId, migrationId, reversed);
        this.propertyOverrideMap = propertyOverrideMap;
        this.targetNodeId = targetNodeId;
    }

    public Map<String, Serializable> getPropertyOverrideMap() {
        return propertyOverrideMap;
    }

    public int getTargetNodeId() {
        return targetNodeId;
    }

    public boolean isForNode(int nodeId) {
        return targetNodeId == nodeId || targetNodeId == UNUSED_CLUSTER_ID;
    }
}
