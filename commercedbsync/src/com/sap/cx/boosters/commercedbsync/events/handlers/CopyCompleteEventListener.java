/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.events.handlers;

import com.sap.cx.boosters.commercedbsync.events.CopyCompleteEvent;
import de.hybris.platform.servicelayer.event.impl.AbstractEventListener;
import de.hybris.platform.tx.Transaction;
import de.hybris.platform.tx.TransactionBody;
import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceProfiler;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Receives an Event when a node has completed Copying Data Tasks
 */
public class CopyCompleteEventListener extends AbstractEventListener<CopyCompleteEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(CopyCompleteEventListener.class.getName());

    private MigrationContext migrationContext;

    private DatabaseCopyTaskRepository databaseCopyTaskRepository;

    private PerformanceProfiler performanceProfiler;

    private ArrayList<MigrationPostProcessor> postProcessors;

	@Override
	protected void onEvent(final CopyCompleteEvent event) {
		final String migrationId = event.getMigrationId();

		LOG.info("Migration finished on Node " + event.getSourceNodeId() + " with result " + event.getCopyResult());
		final CopyContext copyContext = new CopyContext(migrationId, migrationContext, new HashSet<>(),
				performanceProfiler);

        executePostProcessors(copyContext);
    }

    /**
     * Runs through all the Post Processors in a transaction to avoid multiple executions
     *
     * @param copyContext
     */
    private void executePostProcessors(final CopyContext copyContext) {
        try {
            Transaction.current().execute(new TransactionBody() {
                @Override
                public Object execute() throws Exception {

					final MigrationStatus status = databaseCopyTaskRepository.getMigrationStatus(copyContext);

					// ORACLE_TARGET -- START
					if (status.isFailed()) {
						// return null;
						LOG.error("Status FAILED");
					}
					// ORACLE_TARGET -- END

					LOG.debug("Starting PostProcessor execution");

					// ORACLE_TARGET -- START
					if ((status.getStatus() == MigrationProgress.PROCESSED)
							|| (status.getStatus() == MigrationProgress.ABORTED)) {
						postProcessors.forEach(p -> p.process(copyContext));
					}
					// ORACLE_TARGET -- END
					LOG.debug("Finishing PostProcessor execution");

					databaseCopyTaskRepository.setMigrationStatus(copyContext, MigrationProgress.PROCESSED,
							MigrationProgress.COMPLETED);
					return null;
				}
			});
		} catch (final Exception e) {
			if (e instanceof RuntimeException) {
				LOG.error("Error during PostProcessor execution", e);
				throw (RuntimeException) e;
			} else {
				LOG.error("Error during PostProcessor execution", e);
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

	public void setPostProcessors(final ArrayList<MigrationPostProcessor> postProcessors) {
		this.postProcessors = postProcessors;
	}
}
