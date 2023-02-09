/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events.handlers;

import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.events.CopyCompleteEvent;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceProfiler;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;

import de.hybris.platform.servicelayer.event.impl.AbstractEventListener;
import de.hybris.platform.tx.Transaction;
import de.hybris.platform.tx.TransactionBody;

/**
 * Receives an Event when a node has completed Copying Data Tasks
 */
public class CopyCompleteEventListener extends AbstractEventListener<CopyCompleteEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(CopyCompleteEventListener.class.getName());

    private MigrationContext migrationContext;

    private DatabaseCopyTaskRepository databaseCopyTaskRepository;

    private PerformanceProfiler performanceProfiler;

    private List<MigrationPostProcessor> postProcessors;

	@Override
	protected void onEvent(final CopyCompleteEvent event) {
		final String migrationId = event.getMigrationId();

		LOG.info("Migration finished on Node {} with result {}", event.getSourceNodeId(), event.getCopyResult());
		final CopyContext copyContext = new CopyContext(migrationId, migrationContext, new HashSet<>(),
				performanceProfiler);

        executePostProcessors(copyContext);
    }

    /**
     * Runs through all the Post Processors in a transaction to avoid multiple executions
     *
     * @param copyContext
     */
	 private void executePostProcessors(final CopyContext copyContext)
	 {
		 try
		 {
			 Transaction.current().execute(new TransactionBody()
			 {
				 @Override
				 public Object execute() throws Exception
				 {

					 final boolean eligibleForPostProcessing = databaseCopyTaskRepository.setMigrationStatus(copyContext,
							 MigrationProgress.PROCESSED, MigrationProgress.POSTPROCESSING)
							 || databaseCopyTaskRepository.setMigrationStatus(copyContext, MigrationProgress.ABORTED,
									 MigrationProgress.POSTPROCESSING);

					 if (eligibleForPostProcessing)
					 {
						 LOG.info("Starting PostProcessor execution");
						 postProcessors.forEach(p -> p.process(copyContext));

						 databaseCopyTaskRepository.setMigrationStatus(copyContext, MigrationProgress.POSTPROCESSING,
								 MigrationProgress.COMPLETED);
						 LOG.info("Finishing PostProcessor execution");
					 }

					 return null;
				 }
			 });
		 }
		 catch (final Exception e)
		 {
			 LOG.error("Error during PostProcessor execution", e);
			 if (e instanceof RuntimeException re)
			 {
				 throw re;
			 }
			 else
			 {
				 throw new RuntimeException(e);
			 }
		 }
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

	public void setPostProcessors(final List<MigrationPostProcessor> postProcessors) {
		this.postProcessors = postProcessors;
	}
}
