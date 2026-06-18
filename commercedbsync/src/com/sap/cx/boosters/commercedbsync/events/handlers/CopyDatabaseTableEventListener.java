/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events.handlers;

import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.events.CopyDatabaseTableEvent;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceProfiler;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseOperationSchedulerAlgorithm;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTask;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationCopyService;
import de.hybris.platform.servicelayer.cluster.ClusterService;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import de.hybris.platform.servicelayer.event.impl.AbstractEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Listener that starts the Migration Process on a given node
 */
public class CopyDatabaseTableEventListener extends AbstractEventListener<CopyDatabaseTableEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(CopyDatabaseTableEventListener.class.getName());

    private final ReentrantLock lock = new ReentrantLock();

    private DatabaseMigrationCopyService databaseMigrationCopyService;
    private DatabaseCopyTaskRepository databaseCopyTaskRepository;
    private DatabaseOperationSchedulerAlgorithm databaseOperationSchedulerAlgorithm;
    private MigrationContext migrationContext;
    private PerformanceProfiler performanceProfiler;
    private ClusterService clusterService;
    private ConfigurationService configurationService;
    private MigrationContext reverseMigrationContext;

    @Override
    protected void onEvent(final CopyDatabaseTableEvent event) {
        final int clusterId = clusterService.getClusterId();

        if (!event.isForNode(clusterId)) {
            LOG.debug("Ignoring event not dedicated for this node (my: {}, target: {})", clusterId,
                    event.getTargetNodeId());
            return;
        }

        final String migrationId = event.getOperationId();
        processPropertyOverrides(event.getPropertyOverrideMap());
        LOG.debug("Handling queue lookup event for migration with ID {}", migrationId);

        try (MDC.MDCCloseable ignored = MDC.putCloseable(CommercedbsyncConstants.MDC_MIGRATIONID, migrationId);
                MDC.MDCCloseable ignored2 = MDC.putCloseable(CommercedbsyncConstants.MDC_CLUSTERID,
                        String.valueOf(clusterId))) {
            CopyContext copyContext = event.isReversed()
                    ? new CopyContext(migrationId, reverseMigrationContext, new LinkedHashSet<>(), performanceProfiler)
                    : new CopyContext(migrationId, migrationContext, new LinkedHashSet<>(), performanceProfiler);

            MigrationStatus status = databaseCopyTaskRepository.getMigrationStatus(copyContext);

            if (status.getStatus() != MigrationProgress.RUNNING) {
                LOG.warn("Migration with ID {} is not running. Actual status: {}", copyContext.getMigrationId(),
                        status.getStatus());
                return;
            }

            lock.lock();
            int maxTasksToHandle = copyContext.getMigrationContext().getMaxParallelTableCopy()
                    - databaseMigrationCopyService.tasksInProcessing(copyContext);

            if (maxTasksToHandle <= 0) {
                LOG.info("Cannot accept new tasks now");
                return;
            }

            Set<DatabaseCopyTask> queuedPendingTasks = databaseCopyTaskRepository.findQueuedPendingTasks(copyContext);
            List<DatabaseCopyTask> copyTableTasks = queuedPendingTasks.stream().filter(this::canProcessCopyTableTask)
                    .limit(maxTasksToHandle).toList();

            if (copyTableTasks.isEmpty()) {
                LOG.debug("No tasks to process left on this node. Total queued tasks: {}", queuedPendingTasks.size());
                return;
            }

            Set<CopyContext.DataCopyItem> items = copyTableTasks.stream()
                    .map(task -> new CopyContext.DataCopyItem(task.getSourcetablename(), task.getTargettablename(),
                            task.getColumnmap(), task.getSourcerowcount(), task.getBatchsize(),
                            task.getChunk() != null
                                    ? new CopyContext.DataCopyItem.ChunkData(task.getChunk().getCurrentChunk(),
                                            task.getChunk().getChunkSize())
                                    : null))
                    .filter(item -> {
                        try {
                            if (databaseCopyTaskRepository.rescheduleTask(copyContext, item.getPipelineName(),
                                    clusterId)) {
                                LOG.debug("Assigned pipeline {} to cluster node: {}", item.getPipelineName(),
                                        clusterId);

                                return true;
                            } else {
                                LOG.debug("Pipeline {} already claimed by another node", item.getPipelineName());
                            }
                        } catch (Exception e) {
                            LOG.warn("Failed to update pipeline {} details", item.getPipelineName(), e);
                        }
                        return false;
                    }).collect(Collectors.toCollection(LinkedHashSet::new));

            if (!items.isEmpty()) {
                copyContext.getCopyItems().addAll(items);
                databaseMigrationCopyService.copyAllAsync(copyContext);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (lock.isLocked()) {
                lock.unlock();
            }
        }
    }

    private boolean canProcessCopyTableTask(DatabaseCopyTask copyTask) {
        return databaseOperationSchedulerAlgorithm.getNodeIds(copyTask.getSourcetablename())
                .contains(clusterService.getClusterId());
    }

    private void processPropertyOverrides(Map<String, Serializable> propertyOverrideMap) {
        propertyOverrideMap.forEach((key, value) -> {
            configurationService.getConfiguration().setProperty(key, String.valueOf(value));
        });
    }

    public void setDatabaseMigrationCopyService(final DatabaseMigrationCopyService databaseMigrationCopyService) {
        this.databaseMigrationCopyService = databaseMigrationCopyService;
    }

    public void setDatabaseCopyTaskRepository(final DatabaseCopyTaskRepository databaseCopyTaskRepository) {
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
    }

    public void setDatabaseOperationSchedulerAlgorithm(
            DatabaseOperationSchedulerAlgorithm databaseOperationSchedulerAlgorithm) {
        this.databaseOperationSchedulerAlgorithm = databaseOperationSchedulerAlgorithm;
    }

    public void setMigrationContext(final MigrationContext migrationContext) {
        this.migrationContext = migrationContext;
    }

    public void setPerformanceProfiler(final PerformanceProfiler performanceProfiler) {
        this.performanceProfiler = performanceProfiler;
    }

    @Override
    public void setClusterService(ClusterService clusterService) {
        super.setClusterService(clusterService);
        this.clusterService = clusterService;
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setReverseMigrationContext(MigrationContext reverseMigrationContext) {
        this.reverseMigrationContext = reverseMigrationContext;
    }
}
