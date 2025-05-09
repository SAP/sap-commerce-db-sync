/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler.impl;

import com.sap.cx.boosters.commercedbsync.adapter.DataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.adapter.impl.ContextualDataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.events.CopyCompleteEvent;
import com.sap.cx.boosters.commercedbsync.events.CopyDatabaseTableEvent;
import com.sap.cx.boosters.commercedbsync.scheduler.ClusterTableSplittingStrategy;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseCopyScheduler;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseOperationSchedulerAlgorithm;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import de.hybris.platform.core.Registry;
import de.hybris.platform.core.Tenant;
import de.hybris.platform.jalo.JaloSession;
import de.hybris.platform.servicelayer.event.EventService;
import de.hybris.platform.util.Config;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.tuple.Pair;
import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.MDC_CLUSTERID;
import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.MDC_PIPELINE;

/**
 * Scheduler for Cluster Based Migrations
 */
public class CustomClusterDatabaseCopyScheduler implements DatabaseCopyScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(CustomClusterDatabaseCopyScheduler.class);

    private EventService eventService;

    private DatabaseCopyTaskRepository databaseCopyTaskRepository;

    private DatabaseOperationSchedulerAlgorithm databaseOperationSchedulerAlgorithm;

    private ClusterTableSplittingStrategy clusterTableSplittingStrategy;

    /**
     * Schedules a Data Copy Task for each table across all the available nodes
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void schedule(CopyContext context) throws Exception {
        databaseOperationSchedulerAlgorithm.reset();

        logMigrationContext(context.getMigrationContext());

        int ownNodeId = databaseOperationSchedulerAlgorithm.getOwnNodeId();
        if (!CollectionUtils.isEmpty(context.getCopyItems())) {
            DataRepositoryAdapter dataRepositoryAdapter = new ContextualDataRepositoryAdapter(
                    context.getMigrationContext().getDataSourceRepository());
            List<Pair<CopyContext.DataCopyItem, Long>> itemsToSchedule = generateSchedulerItemList(context,
                    dataRepositoryAdapter);
            databaseCopyTaskRepository.createMigrationStatus(context, itemsToSchedule.size());
            long itemOrder = 0;
            for (final Pair<CopyContext.DataCopyItem, Long> itemToSchedule : itemsToSchedule) {
                CopyContext.DataCopyItem dataCopyItem = itemToSchedule.getLeft();
                final long sourceRowCount = itemToSchedule.getRight();
                if (sourceRowCount > 0) {
                    final int destinationNodeId = databaseOperationSchedulerAlgorithm.next();
                    databaseCopyTaskRepository.scheduleTask(context, dataCopyItem, ++itemOrder, sourceRowCount,
                            destinationNodeId);
                } else {
                    databaseCopyTaskRepository.scheduleTask(context, dataCopyItem, itemOrder++, sourceRowCount,
                            ownNodeId);
                    databaseCopyTaskRepository.markTaskCompleted(context, dataCopyItem, "0");
                    if (!context.getMigrationContext().isIncrementalModeEnabled()
                            && context.getMigrationContext().isTruncateEnabled()) {
                        context.getMigrationContext().getDataTargetRepository()
                                .truncateTable(dataCopyItem.getTargetItem());
                    }
                }
            }
            startMonitorThread(context);
            final CopyDatabaseTableEvent event = new CopyDatabaseTableEvent(ownNodeId, context.getMigrationId(),
                    context.getPropertyOverrideMap(), context.getMigrationContext().isReversed());
            eventService.publishEvent(event);
        } else {
            throw new IllegalStateException(
                    "No matching tables found to be copied. Please review configuration and adjust inclusions/exclusions if necessary");
        }
    }

    @Override
    public void resumeUnfinishedItems(CopyContext copyContext) throws Exception {
        databaseOperationSchedulerAlgorithm.reset();
        Set<DatabaseCopyTask> failedTasks = databaseCopyTaskRepository.findFailedTasks(copyContext);
        if (failedTasks.isEmpty()) {
            throw new IllegalStateException("No pending failed table copy tasks found to be resumed");
        }

        for (DatabaseCopyTask failedTask : failedTasks) {
            databaseCopyTaskRepository.rescheduleTask(copyContext, failedTask.getPipelinename(),
                    databaseOperationSchedulerAlgorithm.next());
        }
        databaseCopyTaskRepository.resetMigration(copyContext);
        startMonitorThread(copyContext);
        final CopyDatabaseTableEvent event = new CopyDatabaseTableEvent(
                databaseOperationSchedulerAlgorithm.getOwnNodeId(), copyContext.getMigrationId(),
                copyContext.getPropertyOverrideMap(), copyContext.getMigrationContext().isReversed());
        eventService.publishEvent(event);
    }

    private void logMigrationContext(final MigrationContext context) {
        if (Config.getBoolean("migration.log.context.details", true)) {
            context.dumpLog(LOG);
        }
    }

    private List<Pair<CopyContext.DataCopyItem, Long>> generateSchedulerItemList(CopyContext context,
            DataRepositoryAdapter dataRepositoryAdapter) throws Exception {
        List<Pair<CopyContext.DataCopyItem, Long>> pairs = new ArrayList<>();
        for (CopyContext.DataCopyItem copyItem : context.getCopyItems()) {
            LOG.debug("Counting rows in table: {}", copyItem.getSourceItem());
            final long rowCount = dataRepositoryAdapter.getRowCount(context.getMigrationContext(),
                    copyItem.getSourceItem());
            List<Pair<CopyContext.DataCopyItem, Long>> splitItems = clusterTableSplittingStrategy.split(copyItem,
                    rowCount, databaseOperationSchedulerAlgorithm.getNodeIds().size());
            pairs.addAll(splitItems);
            LOG.debug("Found {} rows in table: {}", rowCount, copyItem.getSourceItem());
        }
        // we sort the items to make sure big tables are assigned to nodes in a fair way
        return pairs.stream().sorted(Comparator.comparingLong(customizedTablesOrder(context)))
                .collect(Collectors.toList());
    }

    private ToLongFunction<Pair<CopyContext.DataCopyItem, Long>> customizedTablesOrder(CopyContext context) {
        if (context.getMigrationContext().isTablesOrdered()) {
            final Set<String> first = context.getMigrationContext().getTablesOrderedAsFirst();
            final Set<String> last = context.getMigrationContext().getTablesOrderedAsLast();

            return pair -> first.contains(pair.getKey().getSourceItem())
                    ? -1
                    : last.contains(pair.getKey().getSourceItem()) ? Integer.MAX_VALUE : pair.getRight();
        } else {
            return Pair::getRight;
        }
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
            statusUpdates.sort(Comparator.comparing(DatabaseCopyTask::getLastUpdate)
                    .thenComparing(DatabaseCopyTask::getPipelinename));
            status.setStatusUpdates(statusUpdates);
        }
        return status;
    }

    @Override
    public boolean isAborted(CopyContext context) throws Exception {
        MigrationStatus current = this.databaseCopyTaskRepository.getMigrationStatus(context);
        return MigrationProgress.ABORTED == current.getStatus();
    }

    @Override
    public void abort(CopyContext context) throws Exception {
        this.databaseCopyTaskRepository.setMigrationStatus(context, MigrationProgress.RUNNING,
                MigrationProgress.ABORTED);
    }

    public void setDatabaseCopyTaskRepository(DatabaseCopyTaskRepository databaseCopyTaskRepository) {
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
    }

    public void setDatabaseOperationSchedulerAlgorithm(
            DatabaseOperationSchedulerAlgorithm databaseOperationSchedulerAlgorithm) {
        this.databaseOperationSchedulerAlgorithm = databaseOperationSchedulerAlgorithm;
    }

    public void setClusterTableSplittingStrategy(ClusterTableSplittingStrategy clusterTableSplittingStrategy) {
        this.clusterTableSplittingStrategy = clusterTableSplittingStrategy;
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
                if (context.getMigrationContext().isDeletionEnabled()) {
                    currentState.setDeletionEnabled(true);
                }

                logState(currentState);
                Duration elapsedTillLastUpdate = Duration
                        .between(currentState.getLastUpdate().toInstant(ZoneOffset.UTC), Instant.now());
                int stalledTimeout = context.getMigrationContext().getStalledTimeout();
                if (elapsedTillLastUpdate.compareTo(Duration.of(stalledTimeout, ChronoUnit.SECONDS)) >= 0) {
                    LOG.error("Migration stalled!");
                    databaseCopyTaskRepository.setMigrationStatus(context, MigrationProgress.STALLED);
                }
                Thread.sleep(5000);
            } while (!currentState.isCompleted() && !currentState.isAborted());
        }

        /**
         * Notifies nodes about termination
         */
        private void notifyFinished() {
            final CopyCompleteEvent completeEvent = new CopyCompleteEvent(
                    databaseOperationSchedulerAlgorithm.getOwnNodeId(), context.getMigrationId(),
                    context.getMigrationContext().isReversed());
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
                        MDC.MDCCloseable ignore2 = MDC.putCloseable(MDC_CLUSTERID,
                                String.valueOf(copyTask.getTargetnodeId()))) {
                    if (copyTask.isFailure()) {
                        LOG.error("{}/{} processed. FAILED in {{}}. Cause: {{}} Last Update: {{}}",
                                copyTask.getTargetrowcount(), copyTask.getSourcerowcount(), copyTask.getDuration(),
                                copyTask.getError(), copyTask.getLastUpdate());
                    } else if (copyTask.isCompleted()) {
                        LOG.info("{}/{} processed. Completed in {{}}. Last Update: {{}}", copyTask.getTargetrowcount(),
                                copyTask.getSourcerowcount(), copyTask.getDuration(), copyTask.getLastUpdate());
                    } else {
                        LOG.debug("{}/{} processed. Last Update: {{}}", copyTask.getTargetrowcount(),
                                copyTask.getSourcerowcount(), copyTask.getLastUpdate());
                    }
                }
            }
            LOG.info("{}/{} tables migrated. {} failed. State: {}", status.getCompletedTasks(), status.getTotalTasks(),
                    status.getFailedTasks(), status.getStatus());
            if (status.isCompleted()) {
                String endState = "finished";
                if (status.isFailed()) {
                    endState = "FAILED";
                }
                if (status.getStart() != null && status.getEnd() != null) {
                    LOG.info("Migration {} ({}) in {}", endState, status.getStatus(), DurationFormatUtils
                            .formatDurationHMS(Duration.between(status.getStart(), status.getEnd()).toMillis()));
                } else {
                    LOG.info("Migration {} ({})", endState, status.getStatus());
                }
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
