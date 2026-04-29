/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler.impl;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseOperationSchedulerAlgorithm;
import com.sap.cx.boosters.commercedbsync.scheduler.cluster.ClusterMonitorEvent;
import com.sap.cx.boosters.commercedbsync.scheduler.cluster.ClusterNodeInfoEvent;
import com.sap.cx.boosters.commercedbsync.scheduler.cluster.ClusterNodeLeaveEvent;
import com.sap.cx.boosters.commercedbsync.scheduler.cluster.NodeInfo;
import de.hybris.platform.cluster.ClusterNodeInfo;
import de.hybris.platform.cluster.ClusterNodeManagementService;
import de.hybris.platform.cluster.DefaultClusterNodeManagementService;
import de.hybris.platform.core.Registry;
import de.hybris.platform.core.Tenant;
import de.hybris.platform.core.TenantListener;
import de.hybris.platform.servicelayer.event.EventService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static de.hybris.platform.core.MasterTenant.MASTERTENANT_ID;

public class ClusterAwareSchedulerAlgorithm implements DatabaseOperationSchedulerAlgorithm {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterAwareSchedulerAlgorithm.class);

    private final Supplier<Object> stalesNodesCleaner = Suppliers.memoizeWithExpiration(this::removeStaleClusterNodes,
            5, TimeUnit.MINUTES);
    private final Map<Integer, NodeInfo> clusterNodes = Maps.newConcurrentMap();
    private final Map<String, Integer> groupIndices = new HashMap<>();

    private final MigrationContext migrationContext;
    private final EventService eventService;

    private ClusterNodeManagementService clusterNodeManagementService;

    private List<Integer> nodeIds = null;
    private int nodeIndex = 0;

    public ClusterAwareSchedulerAlgorithm(MigrationContext migrationContext, EventService eventService) {
        this.migrationContext = migrationContext;
        this.eventService = eventService;
    }

    @Override
    public int getOwnNodeId() {
        return clusterNodeManagementService.getClusterID();
    }

    private Set<String> getOwnGroups() {
        return Sets.newHashSet(clusterNodeManagementService.getClusterGroups());
    }

    @Override
    public List<Integer> getNodeIds(String tableName) {
        if (migrationContext.isClusterMode()) {
            String clusterNodeGroup = migrationContext.getClusterNodeGroup(tableName);

            if (StringUtils.isNotEmpty(clusterNodeGroup)) {
                nodeIds = getNodeIdsForGroup(clusterNodeGroup);

                if (nodeIds.isEmpty()) {
                    nodeIds = null;

                    throw new IllegalStateException(
                            "There are no available nodes assigned to cluster group: " + clusterNodeGroup);
                }
            } else {
                LOG.debug("No cluster group specified for table '{}', using all available cluster nodes", tableName);
            }
        }

        if (nodeIds == null) {
            nodeIds = ImmutableList.copyOf(clusterNodes.keySet());
        }

        return nodeIds;
    }

    @Override
    public int next(String tableName) {
        if (migrationContext.isClusterMode()) {
            String clusterNodeGroup = migrationContext.getClusterNodeGroup(tableName);

            if (StringUtils.isNotEmpty(clusterNodeGroup)) {
                List<Integer> nodeIdsForGroup = getNodeIdsForGroup(clusterNodeGroup);

                if (nodeIdsForGroup.isEmpty()) {
                    throw new IllegalStateException(
                            "There are no available nodes assigned to cluster group: %s [required by table: %s]"
                                    .formatted(clusterNodeGroup, tableName));
                }

                return nodeIdsForGroup.get(groupIndices.compute(clusterNodeGroup,
                        (k, v) -> (v == null || v >= nodeIdsForGroup.size()) ? 0 : v + 1));
            }
        }

        List<Integer> nodeIds = getNodeIds(tableName);

        if (nodeIds.isEmpty()) {
            throw new IllegalStateException("No available cluster nodes for table: " + tableName);
        }

        if (nodeIndex >= (nodeIds.size())) {
            nodeIndex = 0;
        }

        return nodeIds.get(nodeIndex++);
    }

    public void reset() {
        groupIndices.clear();
        nodeIds = null;
        nodeIndex = 0;
    }

    private List<Integer> getNodeIdsForGroup(String clusterGroup) {
        if (StringUtils.isEmpty(clusterGroup)) {
            return List.of();
        }

        stalesNodesCleaner.get();

        return clusterNodes.entrySet().stream().filter(entry -> entry.getValue().matchesGroups(clusterGroup))
                .map(Map.Entry::getKey).sorted().toList();
    }

    @SuppressWarnings("unused")
    public String logClusterInfo() {
        return logClusterInfo(true);
    }

    public String logClusterInfo(boolean log) {
        final String nodesInfo = clusterNodes
                .entrySet().stream().map(e -> " - ID: %d [%s] groups: %s".formatted(e.getKey(),
                        e.getValue().getHostname(), e.getValue().getGroupsAsString()))
                .collect(Collectors.joining("\n"));

        if (log) {
            LOG.info("Cluster details:\n{}", nodesInfo);
            return null;
        }

        return nodesInfo;
    }

    @PostConstruct
    public void init() {
        clusterNodeManagementService = DefaultClusterNodeManagementService.getInstance();

        clusterNodes.put(getOwnNodeId(), new NodeInfo(getOwnGroups()));

        if (migrationContext.isClusterMode()) {
            LOG.debug("Registering cluster notification listener to gather cluster details");

            eventService.registerEventListener(
                    new com.sap.cx.boosters.commercedbsync.scheduler.impl.ClusterAwareSchedulerAlgorithm.ClusterNotificationEventListener());

            Registry.registerTenantListener(new TenantListener() {
                @Override
                public void afterTenantStartUp(Tenant tenant) {
                    if (MASTERTENANT_ID.equals(tenant.getTenantID())) {
                        eventService.publishEvent(new ClusterNodeInfoEvent(new NodeInfo(getOwnGroups())));
                    }
                }

                @Override
                public void beforeTenantShutDown(Tenant tenant) {
                    if (MASTERTENANT_ID.equals(tenant.getTenantID())) {
                        eventService.publishEvent(new ClusterNodeLeaveEvent());
                    }
                }

                @Override
                public void afterSetActivateSession(Tenant tenant) {
                }

                @Override
                public void beforeUnsetActivateSession(Tenant tenant) {
                }
            });
        }
    }

    private Object removeStaleClusterNodes() {
        final long lastPingForStaleNodes = System.currentTimeMillis()
                - clusterNodeManagementService.getStaleNodeTimeout();
        final List<Integer> staleNodes = clusterNodeManagementService.findAllNodes().stream()
                .filter(node -> node.getLastPingTS() < lastPingForStaleNodes).map(ClusterNodeInfo::getId).toList();

        if (!staleNodes.isEmpty()) {
            staleNodes.forEach(clusterNodes::remove);
            reset();

            LOG.debug("Removed stale cluster nodes: {}", staleNodes);
        }

        return null;
    }

    private final class ClusterNotificationEventListener implements ApplicationListener<ClusterMonitorEvent> {
        @Override
        public void onApplicationEvent(@Nonnull ClusterMonitorEvent event) {
            int clusterId = event.getScope().getClusterId();

            if (event instanceof ClusterNodeInfoEvent nodeInfoEvent) {
                if (getOwnNodeId() == clusterId) {
                    return;
                } else if (!nodeInfoEvent.isResponse()) {
                    eventService.publishEvent(new ClusterNodeInfoEvent(new NodeInfo(getOwnGroups()), clusterId));
                }

                if (!clusterNodes.containsKey(clusterId)) {
                    clusterNodes.put(clusterId, nodeInfoEvent.getInfo());

                    LOG.debug("New cluster node information received - ID: {} [{}], groups: {}", clusterId,
                            nodeInfoEvent.getInfo().getHostname(), nodeInfoEvent.getInfo().getGroupsAsString());
                }
            } else if (event instanceof ClusterNodeLeaveEvent) {
                if (clusterNodes.remove(clusterId) != null) {
                    LOG.debug("Node has left the cluster - ID: {}", clusterId);
                }
            }
        }
    }
}
