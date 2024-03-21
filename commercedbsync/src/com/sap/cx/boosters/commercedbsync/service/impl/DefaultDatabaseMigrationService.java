/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import de.hybris.platform.task.TaskEngine;
import de.hybris.platform.task.TaskService;
import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.MigrationReport;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.LaunchOptions;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.context.validation.MigrationContextValidator;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceProfiler;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPreProcessor;
import com.sap.cx.boosters.commercedbsync.provider.CopyItemProvider;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseCopyScheduler;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationReportService;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationService;
import com.sap.cx.boosters.commercedbsync.service.DatabaseSchemaDifferenceService;

public class DefaultDatabaseMigrationService implements DatabaseMigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseMigrationService.class);

    private DatabaseCopyScheduler databaseCopyScheduler;
    private CopyItemProvider copyItemProvider;
    private PerformanceProfiler performanceProfiler;
    private DatabaseMigrationReportService databaseMigrationReportService;
    private DatabaseSchemaDifferenceService schemaDifferenceService;
    private MigrationContextValidator migrationContextValidator;
    private TaskService taskService;
    private DatabaseCopyTaskRepository databaseCopyTaskRepository;
    private ArrayList<MigrationPreProcessor> preProcessors;

    @Override
    public String startMigration(final MigrationContext context, LaunchOptions launchOptions) throws Exception {
        migrationContextValidator.validateContext(context);

        final MigrationStatus runningMigrationStatus = databaseCopyTaskRepository.getRunningMigrationStatus(context);

        if (runningMigrationStatus != null && runningMigrationStatus.getStatus() == MigrationProgress.RUNNING) {
            LOG.debug("Found already running migration with ID: {}", runningMigrationStatus.getMigrationID());

            return runningMigrationStatus.getMigrationID();
        }

        if (!context.isDataExportEnabled()) {
            TaskEngine engine = taskService.getEngine();
            boolean running = engine.isRunning();

            if (running) {
                throw new Exception("Task engine is activated - migration is blocked");
            }
        }

        performanceProfiler.reset();

        if (context.isLogSql()) {
            context.getDataSourceRepository().clearJdbcQueriesStore();
            context.getDataTargetRepository().clearJdbcQueriesStore();
        }

        final String migrationId = UUID.randomUUID().toString();

        MDC.put(CommercedbsyncConstants.MDC_MIGRATIONID, migrationId);

        if (context.isSchemaMigrationEnabled() && context.isSchemaMigrationAutoTriggerEnabled()) {
            schemaDifferenceService.executeSchemaDifferences(context);
        }

        CopyContext copyContext = buildCopyContext(context, migrationId);

        copyContext.getPropertyOverrideMap().putAll(launchOptions.getPropertyOverrideMap());

        preProcessors.stream().filter(p -> p.shouldExecute(copyContext)).forEach(p -> p.process(copyContext));

        databaseCopyScheduler.schedule(copyContext);

        return migrationId;
    }

    @Override
    public void resumeUnfinishedMigration(MigrationContext context, LaunchOptions launchOptions, String migrationID)
            throws Exception {
        CopyContext copyContext = buildIdContext(context, migrationID);
        copyContext.getPropertyOverrideMap().putAll(launchOptions.getPropertyOverrideMap());
        databaseCopyScheduler.resumeUnfinishedItems(copyContext);
    }

    @Override
    public void stopMigration(MigrationContext context, String migrationID) throws Exception {
        CopyContext copyContext = buildIdContext(context, migrationID);
        databaseCopyScheduler.abort(copyContext);
    }

    @Override
    public void markRemainingTasksAborted(MigrationContext context, String migrationID) throws Exception {
        CopyContext copyContext = buildIdContext(context, migrationID);
        databaseCopyTaskRepository.markRemainingTasksAborted(copyContext);
    }

    private CopyContext buildCopyContext(MigrationContext context, String migrationID) throws Exception {
        Set<CopyContext.DataCopyItem> dataCopyItems = copyItemProvider.get(context);
        return new CopyContext(migrationID, context, dataCopyItems, performanceProfiler);
    }

    private CopyContext buildIdContext(MigrationContext context, String migrationID) {
        // we use a lean implementation of the copy context to avoid calling the
        // provider which is not required for task management.
        return new CopyContext.IdCopyContext(migrationID, context, performanceProfiler);
    }

    @Override
    public MigrationStatus getMigrationState(MigrationContext context, String migrationID) throws Exception {
        return getMigrationState(context, migrationID, OffsetDateTime.MAX);
    }

    @Override
    public MigrationStatus getMigrationState(MigrationContext context, String migrationID, OffsetDateTime since)
            throws Exception {
        CopyContext copyContext = buildIdContext(context, migrationID);
        return databaseCopyScheduler.getCurrentState(copyContext, since);
    }

    @Override
    public MigrationReport getMigrationReport(MigrationContext context, String migrationID) throws Exception {
        CopyContext copyContext = buildIdContext(context, migrationID);
        return databaseMigrationReportService.getMigrationReport(copyContext);
    }

    @Override
    public String getMigrationID(MigrationContext context) {
        return databaseCopyTaskRepository.getMostRecentMigrationID(context);
    }

    @Override
    public MigrationStatus waitForFinish(MigrationContext context, String migrationID) throws Exception {
        MigrationStatus status;
        do {
            status = getMigrationState(context, migrationID);
            Thread.sleep(5000);
        } while (!status.isCompleted());

        if (status.isFailed()) {
            throw new Exception("Database migration failed");
        }

        return status;
    }

    public void setDatabaseCopyScheduler(DatabaseCopyScheduler databaseCopyScheduler) {
        this.databaseCopyScheduler = databaseCopyScheduler;
    }

    public void setCopyItemProvider(CopyItemProvider copyItemProvider) {
        this.copyItemProvider = copyItemProvider;
    }

    public void setPerformanceProfiler(PerformanceProfiler performanceProfiler) {
        this.performanceProfiler = performanceProfiler;
    }

    public void setDatabaseMigrationReportService(DatabaseMigrationReportService databaseMigrationReportService) {
        this.databaseMigrationReportService = databaseMigrationReportService;
    }

    public void setSchemaDifferenceService(DatabaseSchemaDifferenceService schemaDifferenceService) {
        this.schemaDifferenceService = schemaDifferenceService;
    }

    public void setMigrationContextValidator(MigrationContextValidator migrationContextValidator) {
        this.migrationContextValidator = migrationContextValidator;
    }

    public void setTaskService(TaskService taskService) {
        this.taskService = taskService;
    }

    public void setDatabaseCopyTaskRepository(DatabaseCopyTaskRepository databaseCopyTaskRepository) {
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
    }

    public void setPreProcessors(final ArrayList<MigrationPreProcessor> preProcessors) {
        this.preProcessors = preProcessors;
    }
}
