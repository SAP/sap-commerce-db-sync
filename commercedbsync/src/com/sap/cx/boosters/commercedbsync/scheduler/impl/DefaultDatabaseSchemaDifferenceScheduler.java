/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler.impl;

import com.sap.cx.boosters.commercedbsync.SchemaDifferenceProgress;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.context.SchemaDifferenceContext;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseSchemaDifferenceScheduler;
import com.sap.cx.boosters.commercedbsync.service.DatabaseSchemaDifferenceTaskRepository;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.platform.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

public class DefaultDatabaseSchemaDifferenceScheduler implements DatabaseSchemaDifferenceScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseSchemaDifferenceScheduler.class);

    private DatabaseSchemaDifferenceTaskRepository databaseSchemaDifferenceTaskRepository;

    private void prepare(final MigrationContext context) {
        final DataRepository repository = context.getDataRepository();
        final DataBaseProvider databaseProvider = repository.getDatabaseProvider();
        final ClassPathResource scriptResource = new ClassPathResource(
                String.format("/sql/createSchemaSchedulerTables%s.sql", databaseProvider));

        if (!scriptResource.exists()) {
            throw new IllegalStateException(
                    "Scheduler tables creation script for database " + databaseProvider + " not found!");
        }

        repository.runSqlScript(scriptResource);
    }

    @Override
    public void schedule(SchemaDifferenceContext context) throws Exception {
        logMigrationContext(context.getMigrationContext());
        prepare(context.getMigrationContext());

        databaseSchemaDifferenceTaskRepository.createSchemaDifferenceStatus(context);
        databaseSchemaDifferenceTaskRepository.scheduleTask(context, "read-source-db");
        databaseSchemaDifferenceTaskRepository.scheduleTask(context, "read-target-db");
        databaseSchemaDifferenceTaskRepository.scheduleTask(context, "compute-diff");
        databaseSchemaDifferenceTaskRepository.scheduleTask(context, "generate-sql");
    }

    @Override
    public void abort(SchemaDifferenceContext context) throws Exception {
        databaseSchemaDifferenceTaskRepository.setSchemaDifferenceStatus(context, SchemaDifferenceProgress.ABORTED);
    }

    private void logMigrationContext(final MigrationContext context) {
        if (Config.getBoolean("migration.log.context.details", true) && context != null) {
            context.dumpLog(LOG);
        }
    }

    public void setDatabaseSchemaDifferenceTaskRepository(
            DatabaseSchemaDifferenceTaskRepository databaseSchemaDifferenceTaskRepository) {
        this.databaseSchemaDifferenceTaskRepository = databaseSchemaDifferenceTaskRepository;
    }
}
