/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */
package com.sap.cx.boosters.commercedbsync.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.sap.cx.boosters.commercedbsync.MigrationReport;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
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
    private ArrayList<MigrationPreProcessor> preProcessors;

    @Override
    public String startMigration(final MigrationContext context) throws Exception {
        migrationContextValidator.validateContext(context);
        performanceProfiler.reset();

        final String migrationId = UUID.randomUUID().toString();

        MDC.put(CommercedbsyncConstants.MDC_MIGRATIONID, migrationId);

        if (context.isSchemaMigrationEnabled() && context.isSchemaMigrationAutoTriggerEnabled()) {
            schemaDifferenceService.executeSchemaDifferences(context);
        }

        CopyContext copyContext = buildCopyContext(context, migrationId);
        
        preProcessors.forEach(p -> p.process(copyContext));
        
        databaseCopyScheduler.schedule(copyContext);

        return migrationId;
    }

	@Override
    public void stopMigration(MigrationContext context, String migrationID) throws Exception {
        CopyContext copyContext = buildIdContext(context, migrationID);
        databaseCopyScheduler.abort(copyContext);
    }

    private CopyContext buildCopyContext(MigrationContext context, String migrationID) throws Exception {
        Set<CopyContext.DataCopyItem> dataCopyItems = copyItemProvider.get(context);
        return new CopyContext(migrationID, context, dataCopyItems, performanceProfiler);
    }

    private CopyContext buildIdContext(MigrationContext context, String migrationID) throws Exception {
        //we use a lean implementation of the copy context to avoid calling the provider which is not required for task management.
        return new CopyContext.IdCopyContext(migrationID, context, performanceProfiler);
    }

    @Override
    public MigrationStatus getMigrationState(MigrationContext context, String migrationID) throws Exception {
        return getMigrationState(context, migrationID, OffsetDateTime.MAX);
    }

    @Override
    public MigrationStatus getMigrationState(MigrationContext context, String migrationID, OffsetDateTime since) throws Exception {
        CopyContext copyContext = buildIdContext(context, migrationID);
        return databaseCopyScheduler.getCurrentState(copyContext, since);
    }

    @Override
    public MigrationReport getMigrationReport(MigrationContext context, String migrationID) throws Exception {
        CopyContext copyContext = buildIdContext(context, migrationID);
        return databaseMigrationReportService.getMigrationReport(copyContext);
    }

	 @Override
	 public String getMigrationID(final MigrationContext migrationContext)
	 {
		 String migrationId = null;
		 try (Connection conn = migrationContext.getDataTargetRepository().getConnection();
				 PreparedStatement stmt = conn.prepareStatement("SELECT migrationId FROM MIGRATIONTOOLKIT_TABLECOPYSTATUS"))
		 {
			 final ResultSet rs = stmt.executeQuery();
			 if (rs.next())
			 {
				 migrationId = rs.getString("migrationId");
			 }
		 }
		 catch (final Exception e)
		 {
			 LOG.error("Couldn't fetch migrationId", e);
		 }
		 return migrationId;
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
    
 	public void setPreProcessors(final ArrayList<MigrationPreProcessor> preProcessors) {
		this.preProcessors = preProcessors;
	}    
}
