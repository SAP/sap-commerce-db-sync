/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler.impl;

import com.google.common.collect.ImmutableList;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseCopySchedulerAlgorithm;
import de.hybris.platform.cluster.PingBroadcastHandler;
import de.hybris.platform.servicelayer.cluster.ClusterService;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RoundRobinClusterSchedulerAlgorithm implements DatabaseCopySchedulerAlgorithm {

    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinClusterSchedulerAlgorithm.class);

    private final MigrationContext migrationContext;

    private final ClusterService clusterService;

    private List<Integer> nodeIds = null;

    private int nodeIndex = 0;

    public RoundRobinClusterSchedulerAlgorithm(MigrationContext migrationContext, ClusterService clusterService) {
        this.migrationContext = migrationContext;
        this.clusterService = clusterService;
    }

    @Override
    public int getOwnNodeId() {
        return clusterService.getClusterId();
    }

    @Override
    public List<Integer> getNodeIds() {
        if (nodeIds == null) {
            nodeIds = ImmutableList.copyOf(detectClusterNodes());
        }
        return nodeIds;
    }

    @Override
    public int next() {
        if (nodeIndex >= (getNodeIds().size())) {
            nodeIndex = 0;
        }
        return getNodeIds().get(nodeIndex++);
    }

    public void reset() {
        nodeIds = null;
        nodeIndex = 0;
    }

    private List<Integer> detectClusterNodes() {
        if (!migrationContext.isClusterMode()) {
            return Collections.singletonList(clusterService.getClusterId());
        }
        final List<Integer> nodeIdList = new ArrayList<>();
        try {
            // Same code as the hac cluster overview page
            PingBroadcastHandler pingBroadcastHandler = PingBroadcastHandler.getInstance();
            pingBroadcastHandler.getNodes().forEach(i -> nodeIdList.add(i.getNodeID()));
        } catch (final Exception e) {
            LOG.warn(
                    "Using single cluster node because an error was encountered while fetching cluster nodes information: {{}}",
                    e.getMessage(), e);
        }
        if (CollectionUtils.isEmpty(nodeIdList)) {
            nodeIdList.add(clusterService.getClusterId());
        }
        return nodeIdList;
    }
}
