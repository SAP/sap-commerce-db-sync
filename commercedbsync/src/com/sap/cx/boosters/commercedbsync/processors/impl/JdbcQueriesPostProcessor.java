/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Post-processor producing and storing reports on the JDBC queries that were
 * executed during a migration against the source and target data repositories.
 */
public class JdbcQueriesPostProcessor implements MigrationPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcQueriesPostProcessor.class.getName());

    @Override
    public void process(CopyContext context) {
        try {
            context.getMigrationContext().getDataSourceRepository().getJdbcQueriesStore()
                    .writeToLogFileAndCompress(context.getMigrationId());
            context.getMigrationContext().getDataTargetRepository().getJdbcQueriesStore()
                    .writeToLogFileAndCompress(context.getMigrationId());
            LOG.info("Finished writing jdbc entries report");
        } catch (Exception e) {
            LOG.error("Error executing post processor", e);
        }
    }

    @Override
    public boolean shouldExecute(CopyContext context) {
        return context.getMigrationContext().isLogSql();
    }
}
