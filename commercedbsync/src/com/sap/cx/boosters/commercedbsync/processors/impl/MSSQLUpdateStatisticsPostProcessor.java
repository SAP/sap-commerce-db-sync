/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTask;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <b>Update statistics for MS SQL tables post processor</b> <br/>
 * <br/>
 * Runs UPDATE STATISTICS against all migrated tables in the MS SQL target
 * database after successful data migration. This post processor can be
 * enabled/disabled by setting property
 * `migration.data.mssql.update.statistics.enabled` to true/false.
 */
public class MSSQLUpdateStatisticsPostProcessor implements MigrationPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(MSSQLUpdateStatisticsPostProcessor.class);
    private static final String UPDATE_STATISTICS_STATEMENT = "UPDATE STATISTICS %s";
    private DatabaseCopyTaskRepository databaseCopyTaskRepository;

    @Override
    public void process(CopyContext context) {
        final DataRepository targetRepository = context.getMigrationContext().getDataTargetRepository();

        try (Connection connection = targetRepository.getConnection()) {
            final List<String> migratedTables = databaseCopyTaskRepository.getAllTasks(context).stream()
                    .map(DatabaseCopyTask::getTargettablename).collect(Collectors.toList());

            try (Statement statement = connection.createStatement()) {
                for (final String table : migratedTables) {
                    statement.addBatch(String.format(UPDATE_STATISTICS_STATEMENT, table));
                }
                statement.executeBatch();

                LOG.info("Successfully executed SQL update statistics statement");
            } catch (SQLException e) {
                LOG.error("Error executing SQL update statistics statement", e);
            }

        } catch (Exception e) {
            LOG.error("Error executing post processor", e);
        }
    }

    @Override
    public boolean shouldExecute(CopyContext context) {
        if (context.getMigrationContext().isMssqlUpdateStatisticsEnabled()) {

            MigrationStatus status;
            try {
                status = databaseCopyTaskRepository.getMigrationStatus(context);
            } catch (Exception e) {
                LOG.error("Error executing post processor", e);
                return false;
            }

            return !status.isFailed() && !context.getMigrationContext().isDataSynchronizationEnabled()
                    && context.getMigrationContext().getDataTargetRepository().getDatabaseProvider().isMssqlUsed();

        }

        return false;
    }

    public void setDatabaseCopyTaskRepository(final DatabaseCopyTaskRepository databaseCopyTaskRepository) {
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
    }
}
