/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler.cluster;

public final class ClusterNodeInfoEvent extends ClusterMonitorEvent {
    private final NodeInfo info;

    public ClusterNodeInfoEvent(NodeInfo info) {
        this(info, null);
    }

    public ClusterNodeInfoEvent(NodeInfo info, Integer target) {
        super(target);
        this.info = info;
    }

    public NodeInfo getInfo() {
        return info;
    }
}
