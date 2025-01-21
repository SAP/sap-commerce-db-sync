/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events;

import de.hybris.platform.servicelayer.event.ClusterAwareEvent;
import de.hybris.platform.servicelayer.event.PublishEventContext;
import de.hybris.platform.servicelayer.event.events.AbstractEvent;

/**
 * ClusterAwareEvent to notify other Nodes to start the operation
 */
public abstract class OperationEvent extends AbstractEvent implements ClusterAwareEvent {
    private final int sourceNodeId;
    private final String operationId;
    private final boolean reversed;

    public OperationEvent(final int sourceNodeId, final String operationId, final boolean reversed) {
        super();
        this.sourceNodeId = sourceNodeId;
        this.operationId = operationId;
        this.reversed = reversed;
    }

    @Override
    public boolean canPublish(PublishEventContext publishEventContext) {
        return true;
    }

    /**
     * @return the masterNodeId
     */
    public int getSourceNodeId() {
        return sourceNodeId;
    }

    /**
     * @return the operationId
     */
    public String getOperationId() {
        return operationId;
    }

    /**
     * @return true if reversed
     */
    public boolean isReversed() {
        return reversed;
    }
}
