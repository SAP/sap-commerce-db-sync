/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */
package com.sap.cx.boosters.commercedbsync.events.handlers;

import com.sap.cx.boosters.commercedbsync.events.CopyDatabaseTableEvent;
import de.hybris.platform.servicelayer.cluster.ClusterService;
import de.hybris.platform.servicelayer.event.impl.AbstractEventListener;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceProfiler;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTask;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationCopyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.MDC_CLUSTERID;
import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.MDC_MIGRATIONID;

/**
 * Listener that starts the Migration Process on a given node
 */
public class CopyDatabaseTableEventListener extends AbstractEventListener<CopyDatabaseTableEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(CopyDatabaseTableEventListener.class.getName());

    private DatabaseMigrationCopyService databaseMigrationCopyService;

    private DatabaseCopyTaskRepository databaseCopyTaskRepository;

    private MigrationContext migrationContext;

    private PerformanceProfiler performanceProfiler;

    private ClusterService clusterService;


    @Override
    protected void onEvent(final CopyDatabaseTableEvent event) {
        final String migrationId = event.getMigrationId();

        LOG.debug("Starting Migration with Id {}", migrationId);
        try (MDC.MDCCloseable ignored = MDC.putCloseable(MDC_MIGRATIONID, migrationId);
             MDC.MDCCloseable ignored2 = MDC.putCloseable(MDC_CLUSTERID, String.valueOf(clusterService.getClusterId()))
        ) {
            CopyContext copyContext = new CopyContext(migrationId, migrationContext, new HashSet<>(), performanceProfiler);
            Set<DatabaseCopyTask> copyTableTasks = databaseCopyTaskRepository.findPendingTasks(copyContext);
            Set<CopyContext.DataCopyItem> items = copyTableTasks.stream().map(task -> new CopyContext.DataCopyItem(task.getSourcetablename(), task.getTargettablename(), task.getColumnmap(), task.getSourcerowcount())).collect(Collectors.toSet());
            copyContext.getCopyItems().addAll(items);
            databaseMigrationCopyService.copyAllAsync(copyContext);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void setDatabaseMigrationCopyService(final DatabaseMigrationCopyService databaseMigrationCopyService) {
        this.databaseMigrationCopyService = databaseMigrationCopyService;
    }

    public void setDatabaseCopyTaskRepository(final DatabaseCopyTaskRepository databaseCopyTaskRepository) {
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
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

}
