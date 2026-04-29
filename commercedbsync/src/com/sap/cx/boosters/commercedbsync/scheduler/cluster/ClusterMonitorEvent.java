/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler.cluster;

import de.hybris.platform.servicelayer.event.ClusterAwareEvent;
import de.hybris.platform.servicelayer.event.PublishEventContext;
import de.hybris.platform.servicelayer.event.events.AbstractEvent;

public abstract class ClusterMonitorEvent extends AbstractEvent implements ClusterAwareEvent {
    private final Integer targetNodeId;

    protected ClusterMonitorEvent() {
        this(null);
    }

    protected ClusterMonitorEvent(Integer targetNodeId) {
        this.targetNodeId = targetNodeId;
    }

    @Override
    public boolean canPublish(PublishEventContext publishEventContext) {
        return !isResponse() || publishEventContext.getTargetNodeId() == targetNodeId;
    }

    public boolean isResponse() {
        return targetNodeId != null;
    }
}
