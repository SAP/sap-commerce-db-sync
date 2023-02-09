/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler.impl;

import com.sap.cx.boosters.commercedbsync.events.CopyCompleteEvent;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseCopyScheduler;
import de.hybris.platform.cluster.PingBroadcastHandler;
import de.hybris.platform.core.Registry;
import de.hybris.platform.core.Tenant;
import de.hybris.platform.jalo.JaloSession;
import de.hybris.platform.servicelayer.cluster.ClusterService;
import de.hybris.platform.servicelayer.event.EventService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.tuple.Pair;
import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.adapter.DataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.adapter.impl.ContextualDataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.events.CopyDatabaseTableEvent;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTask;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import com.sap.cx.boosters.commercedbsync.views.TableViewGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.io.ClassPathResource;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.MDC_CLUSTERID;
import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.MDC_PIPELINE;
import static org.mockito.ArgumentMatchers.contains;

/**
 * Scheduler for Cluster Based Migrations
 */
public class CustomClusterDatabaseCopyScheduler implements DatabaseCopyScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(CustomClusterDatabaseCopyScheduler.class);

    private EventService eventService;

    private ClusterService clusterService;

    private DatabaseCopyTaskRepository databaseCopyTaskRepository;

    /**
     * Schedules a Data Copy Task for each table across all the available nodes
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void schedule(CopyContext context) throws Exception {
	    String sqlScript = "";
		// ORACLE_TARGET - START
		if (context.getMigrationContext().getDataTargetRepository().getDatabaseProvider().isOracleUsed()) {
			sqlScript = "/sql/createSchedulerTablesOracle.sql";
		} else if(context.getMigrationContext().getDataTargetRepository().getDatabaseProvider().isHanaUsed()){
            sqlScript = "/sql/createSchedulerTablesHana.sql";
        } else if(context.getMigrationContext().getDataTargetRepository().getDatabaseProvider().isPostgreSqlUsed()){
            sqlScript = "/sql/createSchedulerTablesPostGres.sql";
        } else {
            sqlScript = "/sql/createSchedulerTables.sql";
        }
        logMigrationContext(context.getMigrationContext());
		// ORACLE_TARGET - END
        context.getMigrationContext().getDataTargetRepository().runSqlScript(new ClassPathResource(sqlScript));
        int ownNodeId = clusterService.getClusterId();
        if (!CollectionUtils.isEmpty(context.getCopyItems())) {
            databaseCopyTaskRepository.createMigrationStatus(context);
            final List<Integer> nodeIds = getClusterNodes(context);
            int nodeIndex = 0;
            DataRepositoryAdapter dataRepositoryAdapter = new ContextualDataRepositoryAdapter(context.getMigrationContext().getDataSourceRepository());
            List<Pair<CopyContext.DataCopyItem, Long>> itemsToSchedule = generateSchedulerItemList(context, dataRepositoryAdapter);
            for (final Pair<CopyContext.DataCopyItem, Long> itemToSchedule : itemsToSchedule) {
                CopyContext.DataCopyItem dataCopyItem = itemToSchedule.getLeft();
                final long sourceRowCount = itemToSchedule.getRight();
                if (sourceRowCount > 0) {
                    if (nodeIndex >= (nodeIds.size())) {
                        nodeIndex = 0;
                    }
                    final int destinationNodeId = nodeIds.get(nodeIndex);
                    databaseCopyTaskRepository.scheduleTask(context, dataCopyItem, sourceRowCount, destinationNodeId);
                    nodeIndex++;
                } else {
                    databaseCopyTaskRepository.scheduleTask(context, dataCopyItem, sourceRowCount, ownNodeId);
                    databaseCopyTaskRepository.markTaskCompleted(context, dataCopyItem, "0");
                    if(!context.getMigrationContext().isIncrementalModeEnabled() && context.getMigrationContext().isTruncateEnabled()) {
                    	context.getMigrationContext().getDataTargetRepository().truncateTable(dataCopyItem.getTargetItem());
                    }
                }
            }
            startMonitorThread(context);
            final CopyDatabaseTableEvent event = new CopyDatabaseTableEvent(ownNodeId, context.getMigrationId());
            eventService.publishEvent(event);
        }
    }

	private void logMigrationContext(final MigrationContext context) {
		if (context == null) {
			return;
		}
		LOG.info("--------MIGRATION CONTEXT- START----------");
		LOG.info("isAddMissingColumnsToSchemaEnabled=" + context.isAddMissingColumnsToSchemaEnabled());
		LOG.info("isAddMissingTablesToSchemaEnabled=" + context.isAddMissingTablesToSchemaEnabled());
		LOG.info("isAuditTableMigrationEnabled=" + context.isAuditTableMigrationEnabled());
		LOG.info("isBulkCopyEnabled=" + context.isBulkCopyEnabled());
		LOG.info("isClusterMode=" + context.isClusterMode());
		LOG.info("isDeletionEnabled=" + context.isDeletionEnabled());
		LOG.info("isDisableAllIndexesEnabled=" + context.isDisableAllIndexesEnabled());
		LOG.info("isDropAllIndexesEnabled=" + context.isDropAllIndexesEnabled());
		LOG.info("isFailOnErrorEnabled=" + context.isFailOnErrorEnabled());
		LOG.info("isIncrementalModeEnabled=" + context.isIncrementalModeEnabled());
		LOG.info("isMigrationTriggeredByUpdateProcess=" + context.isMigrationTriggeredByUpdateProcess());
		LOG.info("isRemoveMissingColumnsToSchemaEnabled=" + context.isRemoveMissingColumnsToSchemaEnabled());
		LOG.info("isRemoveMissingTablesToSchemaEnabled=" + context.isRemoveMissingTablesToSchemaEnabled());
		LOG.info("isSchemaMigrationAutoTriggerEnabled=" + context.isSchemaMigrationAutoTriggerEnabled());
		LOG.info("isSchemaMigrationEnabled=" + context.isSchemaMigrationEnabled());
		LOG.info("isTruncateEnabled=" + context.isTruncateEnabled());
		LOG.info("getIncludedTables=" + context.getIncludedTables());
		LOG.info("getExcludedTables=" + context.getExcludedTables());
		LOG.info("getIncrementalTables=" + context.getIncrementalTables());
		LOG.info("getTruncateExcludedTables=" + context.getTruncateExcludedTables());
		LOG.info("getCustomTables=" + context.getCustomTables());
		LOG.info("getIncrementalTimestamp=" + context.getIncrementalTimestamp());
		LOG.info(
				"Source TS Name=" + context.getDataSourceRepository().getDataSourceConfiguration().getTypeSystemName());
		LOG.info("Source TS Suffix ="
				+ context.getDataSourceRepository().getDataSourceConfiguration().getTypeSystemSuffix());
		LOG.info(
				"Target TS Name=" + context.getDataTargetRepository().getDataSourceConfiguration().getTypeSystemName());
		LOG.info("Target TS Suffix ="
				+ context.getDataTargetRepository().getDataSourceConfiguration().getTypeSystemSuffix());
		LOG.info("getItemTypeViewNamePattern=" + context.getItemTypeViewNamePattern());
		
		
		LOG.info("--------MIGRATION CONTEXT- END----------");
	}
    private List<Pair<CopyContext.DataCopyItem, Long>> generateSchedulerItemList(CopyContext context, DataRepositoryAdapter dataRepositoryAdapter) throws Exception {
        List<Pair<CopyContext.DataCopyItem, Long>> pairs = new ArrayList<>();
        for (CopyContext.DataCopyItem copyItem : context.getCopyItems()) {
            pairs.add(Pair.of(copyItem, dataRepositoryAdapter.getRowCount(context.getMigrationContext(), copyItem.getSourceItem())));
        }
        //we sort the items to make sure big tables are assigned to nodes in a fair way
        return pairs.stream().sorted((p1, p2) -> Long.compare(p1.getRight(), p2.getRight())).collect(Collectors.toList());
    }

    /**
     * Starts a thread to monitor the migration
     *
     * @param context
     */
    private void startMonitorThread(CopyContext context) {
        JaloSession jaloSession = JaloSession.getCurrentSession();

        Thread monitor = new Thread(new MigrationMonitor(context, jaloSession), "MigrationMonitor");
        monitor.start();
    }

    @Override
    public MigrationStatus getCurrentState(CopyContext context, OffsetDateTime since) throws Exception {
        Objects.requireNonNull(context);
        Objects.requireNonNull(since);

        MigrationStatus status = databaseCopyTaskRepository.getMigrationStatus(context);
        if (!since.equals(OffsetDateTime.MAX)) {
            Set<DatabaseCopyTask> updated = databaseCopyTaskRepository.getUpdatedTasks(context, since);
            List<DatabaseCopyTask> statusUpdates = new ArrayList<>(updated);
            statusUpdates.sort(Comparator.comparing(DatabaseCopyTask::getLastUpdate).thenComparing(DatabaseCopyTask::getPipelinename));
            status.setStatusUpdates(statusUpdates);
        }
        return status;
    }

    @Override
    public boolean isAborted(CopyContext context) throws Exception {
        MigrationStatus current = this.databaseCopyTaskRepository.getMigrationStatus(context);
        return MigrationProgress.ABORTED.equals(current.getStatus());
    }

    @Override
    public void abort(CopyContext context) throws Exception {
        this.databaseCopyTaskRepository.setMigrationStatus(context, MigrationProgress.ABORTED);
        stopPerformanceProfiling(context);
    }

    private void stopPerformanceProfiling(CopyContext context) {
        if (context.getPerformanceProfiler() != null) {
            context.getPerformanceProfiler().reset();
        }
    }

    private List<Integer> getClusterNodes(CopyContext context) {
        if (!context.getMigrationContext().isClusterMode()) {
            return Collections.singletonList(clusterService.getClusterId());
        }
        final List<Integer> nodeIds = new ArrayList<>();
        try {
            // Same code as the hac cluster overview page
            PingBroadcastHandler pingBroadcastHandler = PingBroadcastHandler.getInstance();
            pingBroadcastHandler.getNodes().forEach(i -> nodeIds.add(i.getNodeID()));
        } catch (final Exception e) {
            LOG.warn("Using single cluster node because an error was encountered while fetching cluster nodes information: {{}}", e.getMessage(), e);
        }
        if (CollectionUtils.isEmpty(nodeIds)) {
            nodeIds.add(clusterService.getClusterId());
        }
        return nodeIds;
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setDatabaseCopyTaskRepository(DatabaseCopyTaskRepository databaseCopyTaskRepository) {
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
    }

    public void setEventService(EventService eventService) {
        this.eventService = eventService;
    }

    /**
     * Thread to monitor the Migration
     */
    private class MigrationMonitor implements Runnable {
        private final CopyContext context;
        private final Map<String, String> contextMap;
        private final Tenant tenant;
        private final JaloSession jaloSession;
        private OffsetDateTime lastUpdate = OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        public MigrationMonitor(CopyContext context, JaloSession jaloSession) {
            this.context = context;
            this.contextMap = MDC.getCopyOfContextMap();
            this.jaloSession = jaloSession;
            this.tenant = jaloSession.getTenant();

        }

        @Override
        public void run() {
            try {
                prepareThread();
                pollState();
                notifyFinished();
            } catch (Exception e) {
                LOG.error("Failed getting current state", e);
            } finally {
                cleanupThread();
            }
        }

        /**
         * Detects if the migration has stalled
         *
         * @throws Exception
         */
        private void pollState() throws Exception {
            MigrationStatus currentState;
            do {
                currentState = getCurrentState(context, lastUpdate);
                lastUpdate = OffsetDateTime.now(ZoneOffset.UTC);

                // setting deletion
                if(context.getMigrationContext().isDeletionEnabled()){
                    currentState.setDeletionEnabled(true);
                }

                logState(currentState);
                Duration elapsedTillLastUpdate = Duration.between(currentState.getLastUpdate().toInstant(ZoneOffset.UTC), Instant.now());
                int stalledTimeout = context.getMigrationContext().getStalledTimeout();
                if (elapsedTillLastUpdate.compareTo(Duration.of(stalledTimeout, ChronoUnit.SECONDS)) >= 0) {
                    LOG.error("Migration stalled!");
                    databaseCopyTaskRepository.setMigrationStatus(context, MigrationProgress.STALLED);
                }
                Thread.sleep(5000);
            } while (!currentState.isCompleted());
        }

        /**
         * Notifies nodes about termination
         */
        private void notifyFinished() {
            final CopyCompleteEvent completeEvent = new CopyCompleteEvent(clusterService.getClusterId(), context.getMigrationId());
            eventService.publishEvent(completeEvent);
        }

        /**
         * Logs the current migration state
         *
         * @param status
         */
        private void logState(MigrationStatus status) {
            for (final DatabaseCopyTask copyTask : status.getStatusUpdates()) {
                try (MDC.MDCCloseable ignore = MDC.putCloseable(MDC_PIPELINE, copyTask.getPipelinename());
                     MDC.MDCCloseable ignore2 = MDC.putCloseable(MDC_CLUSTERID, String.valueOf(copyTask.getTargetnodeId()))) {
                    if (copyTask.isFailure()) {
                        LOG.error("{}/{} processed. FAILED in {{}}. Cause: {{}} Last Update: {{}}", copyTask.getTargetrowcount(), copyTask.getSourcerowcount(), copyTask.getDuration(), copyTask.getError(), copyTask.getLastUpdate());
                    } else if (copyTask.isCompleted()) {
                        LOG.info("{}/{} processed. Completed in {{}}. Last Update: {{}}", copyTask.getTargetrowcount(), copyTask.getSourcerowcount(), copyTask.getDuration(), copyTask.getLastUpdate());
                    } else {
                        LOG.debug("{}/{} processed. Last Update: {{}}", copyTask.getTargetrowcount(), copyTask.getSourcerowcount(), copyTask.getLastUpdate());
                    }
                }
            }
            LOG.info("{}/{} tables migrated. {} failed. State: {}", status.getCompletedTasks(), status.getTotalTasks(), status.getFailedTasks(), status.getStatus());
            if (status.isCompleted()) {
                String endState = "finished";
                if (status.isFailed()) {
                    endState = "FAILED";
                }
                LOG.info("Migration {} ({}) in {}", endState, status.getStatus(), DurationFormatUtils.formatDurationHMS(Duration.between(status.getStart(), status.getEnd()).toMillis()));
            }
        }


        protected void prepareThread() {
            MDC.setContextMap(contextMap);

            // tenant
            Registry.setCurrentTenant(tenant);
            // jalo session
            this.jaloSession.activate();
        }

        protected void cleanupThread() {
            MDC.clear();

            // jalo session
            JaloSession.deactivate();
            // tenant
            Registry.unsetCurrentTenant();
        }
    }

}
