/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.strategy.impl;

import com.sap.cx.boosters.commercedbsync.concurrent.DataWorkerExecutor;
import com.sap.cx.boosters.commercedbsync.concurrent.MaybeFinished;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceCategory;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceRecorder;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTask;
import com.sap.cx.boosters.commercedbsync.strategy.PipeWriterStrategy;
import de.hybris.bootstrap.ddl.DataBaseProvider;

import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.DataThreadPoolConfig;
import com.sap.cx.boosters.commercedbsync.concurrent.DataPipe;
import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolConfigBuilder;
import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolFactory;
import com.sap.cx.boosters.commercedbsync.concurrent.impl.task.RetriableTask;
import com.sap.cx.boosters.commercedbsync.concurrent.impl.DefaultDataWorkerExecutor;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class CopyPipeWriterStrategy implements PipeWriterStrategy<DataSet> {
    private static final Logger LOG = LoggerFactory.getLogger(CopyPipeWriterStrategy.class);

    private final DatabaseCopyTaskRepository taskRepository;

    private final DataThreadPoolFactory dataWriteWorkerPoolFactory;

    private static final String LP_SUFFIX = "lp";

    public CopyPipeWriterStrategy(DatabaseCopyTaskRepository taskRepository,
            DataThreadPoolFactory dataWriteWorkerPoolFactory) {
        this.taskRepository = taskRepository;
        this.dataWriteWorkerPoolFactory = dataWriteWorkerPoolFactory;
    }

    @Override
    public void write(CopyContext context, DataPipe<DataSet> pipe, CopyContext.DataCopyItem item) throws Exception {
        final DataBaseProvider dbProvider = context.getMigrationContext().getDataTargetRepository()
                .getDatabaseProvider();
        String targetTableName = item.getTargetItem();
        PerformanceRecorder performanceRecorder = context.getPerformanceProfiler()
                .createRecorder(PerformanceCategory.DB_WRITE, targetTableName);
        performanceRecorder.start();
        Set<String> excludedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (context.getMigrationContext().getExcludedColumns().containsKey(targetTableName)) {
            excludedColumns.addAll(context.getMigrationContext().getExcludedColumns().get(targetTableName));
            LOG.info("Ignoring excluded column(s): {}", excludedColumns);
        }
        Set<String> nullifyColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (context.getMigrationContext().getNullifyColumns().containsKey(targetTableName)) {
            nullifyColumns.addAll(context.getMigrationContext().getNullifyColumns().get(targetTableName));
            LOG.info("Nullify column(s): {}", nullifyColumns);
        }

        List<String> columnsToCopy = new ArrayList<>();
        try (Connection sourceConnection = context.getMigrationContext().getDataSourceRepository().getConnection();
                Statement stmt = sourceConnection.createStatement();
                ResultSet metaResult = stmt
                        .executeQuery(String.format("select * from %s where 0 = 1", item.getSourceItem()))) {
            ResultSetMetaData sourceMeta = metaResult.getMetaData();
            int columnCount = sourceMeta.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String column = sourceMeta.getColumnName(i);
                if (!excludedColumns.contains(column)) {
                    columnsToCopy.add(column);
                }
            }
        }

        if (columnsToCopy.isEmpty()) {
            throw new IllegalStateException(
                    String.format("%s: source has no columns or all columns excluded", item.getPipelineName()));
        }
        DataThreadPoolConfig threadPoolConfig = new DataThreadPoolConfigBuilder(context.getMigrationContext())
                .withPoolSize(context.getMigrationContext().getMaxParallelWriterWorkers()).build();
        ThreadPoolTaskExecutor taskExecutor = dataWriteWorkerPoolFactory.create(context, threadPoolConfig);
        DataWorkerExecutor<Boolean> workerExecutor = new DefaultDataWorkerExecutor<>(taskExecutor);
        Connection targetConnection = null;
        AtomicLong totalCount = new AtomicLong(
                taskRepository.findPipeline(context, item).map(DatabaseCopyTask::getTargetrowcount).orElse(0L));
        List<String> upsertIds = new ArrayList<>();
        try {
            targetConnection = context.getMigrationContext().getDataTargetRepository().getConnection();

            final boolean requiresIdentityInsert = requiresIdentityInsert(item.getTargetItem(), targetConnection,
                    dbProvider, context.getMigrationContext().getDataTargetRepository().getDataSourceConfiguration());
            MaybeFinished<DataSet> sourcePage;
            boolean firstPage = true;
            CopyPipeWriterContext copyPipeWriterContext = null;
            do {
                sourcePage = pipe.get();
                if (sourcePage.isPoison()) {
                    throw new IllegalStateException("Poison received; dying. Check the logs for further insights.");
                }
                DataSet dataSet = sourcePage.getValue();
                if (firstPage) {
                    if (doTruncateIfNecessary(context, item)) {
                        totalCount.set(0);
                        taskRepository.updateTaskProgress(context, item, totalCount.get());
                    }
                    doTurnOnOffIndicesIfNecessary(context, item.getTargetItem(), false);
                    if (context.getMigrationContext().isIncrementalModeEnabled()) {
                        if (context.getMigrationContext().isLpTableMigrationEnabled()
                                && StringUtils.endsWithIgnoreCase(item.getSourceItem(), LP_SUFFIX)) {
                            determineLpUpsertId(upsertIds, dataSet);
                        } else {
                            determineUpsertId(upsertIds, dataSet);
                        }
                    }
                    copyPipeWriterContext = new CopyPipeWriterContext(context, item, columnsToCopy, nullifyColumns,
                            performanceRecorder, totalCount, upsertIds, requiresIdentityInsert, taskRepository);
                    firstPage = false;
                }
                if (dataSet.isNotEmpty()) {
                    RetriableTask writerTask = createWriterTask(copyPipeWriterContext, dataSet);
                    workerExecutor.safelyExecute(writerTask);
                }
            } while (!sourcePage.isDone());
            workerExecutor.waitAndRethrowUncaughtExceptions();
        } catch (Exception e) {
            pipe.requestAbort(e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        } finally {
            if (taskExecutor != null) {
                dataWriteWorkerPoolFactory.destroy(taskExecutor);
            }
            if (targetConnection != null) {
                doTurnOnOffIndicesIfNecessary(context, item.getTargetItem(), true);
                targetConnection.close();
            }

            taskRepository.updateTaskProgress(context, item, totalCount.get());
        }
    }

    private boolean doTruncateIfNecessary(CopyContext context, CopyContext.DataCopyItem item) throws Exception {
        String targetTableName = item.getTargetItem();
        if (context.getMigrationContext().isSchedulerResumeEnabled()) {
            Optional<DatabaseCopyTask> pipeline = taskRepository.findPipeline(context, item);
            if (pipeline.isPresent()) {
                DatabaseCopyTask databaseCopyTask = pipeline.get();
                /*
                 * check if table was initially truncated. Could happen that batches are
                 * scheduled but migration was aborted before truncation.
                 */
                if (databaseCopyTask.isTruncated()) {
                    return false;
                }
            }
        }
        if (context.getMigrationContext().isTruncateEnabled()) {
            if (!context.getMigrationContext().getTruncateExcludedTables().contains(targetTableName)) {
                assertTruncateAllowed(context);
                context.getMigrationContext().getDataTargetRepository().truncateTable(targetTableName);
                taskRepository.markTaskTruncated(context, item);
                return true;
            } else {
                taskRepository.markTaskTruncated(context, item);
            }
        }
        return false;
    }

    protected void doTurnOnOffIndicesIfNecessary(CopyContext context, String targetTableName, boolean on)
            throws Exception {
        if (context.getMigrationContext().isDropAllIndexesEnabled()) {
            if (!on) {
                LOG.debug("{} indexes for table '{}'", "Dropping", targetTableName);
                context.getMigrationContext().getDataTargetRepository().dropIndexesOfTable(targetTableName);
            }
        } else {
            if (context.getMigrationContext().isDisableAllIndexesEnabled()) {
                if (!context.getMigrationContext().getDisableAllIndexesIncludedTables().isEmpty()) {
                    if (!context.getMigrationContext().getDisableAllIndexesIncludedTables().contains(targetTableName)) {
                        return;
                    }
                }
                LOG.debug("{} indexes for table '{}'", on ? "Rebuilding" : "Disabling", targetTableName);
                if (on) {
                    context.getMigrationContext().getDataTargetRepository().enableIndexesOfTable(targetTableName);
                } else {
                    context.getMigrationContext().getDataTargetRepository().disableIndexesOfTable(targetTableName);
                }
            }
        }
    }

    protected void assertTruncateAllowed(CopyContext context) {
        if (context.getMigrationContext().isIncrementalModeEnabled()) {
            throw new IllegalStateException("Truncating tables in incremental mode is illegal. Change the property "
                    + CommercedbsyncConstants.MIGRATION_DATA_TRUNCATE_ENABLED + " to false");
        }
    }

    private void determineUpsertId(List<String> upsertIds, DataSet dataSet) {
        if (dataSet.hasColumn("PK")) {
            upsertIds.add("PK");
            return;
        } else if (dataSet.hasColumn("ID")) {
            upsertIds.add("ID");
        } else {
            // should we support more IDs? In the hybris context there is hardly any other
            // with regards to transactional data.
        }
    }

    private void determineLpUpsertId(List<String> upsertIds, DataSet dataSet) {
        if (dataSet.hasColumn("ITEMPK") && dataSet.hasColumn("LANGPK")) {
            upsertIds.add("ITEMPK");
            upsertIds.add("LANGPK");
        } else {
            // should we support more IDs? In the hybris context there is hardly any other
            // with regards to transactional data.
        }
    }

    private String buildSqlForIdentityInsertCheck(final String targetTableName, final DataBaseProvider dbProvider,
            final DataSourceConfiguration dsConfig) {
        final String sql;
        if (dbProvider.isMssqlUsed()) {
            sql = String.format(
                    "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS BIT FROM sys.columns WHERE object_id = object_id('%s') AND is_identity = 1",
                    targetTableName);
        } else if (dbProvider.isOracleUsed()) {
            sql = String.format(
                    "SELECT has_identity FROM all_tables WHERE UPPER(table_name) = UPPER('%s') AND UPPER(owner) = UPPER('%s')",
                    targetTableName, dsConfig.getSchema());
        } else if (dbProvider.isHanaUsed()) {
            sql = String.format(
                    "SELECT is_insert_only FROM public.tables WHERE table_name = UPPER('%s') AND schema_name = UPPER('%s')",
                    targetTableName, dsConfig.getSchema());
        } else {
            throw new UnsupportedOperationException(
                    "Database type '" + dbProvider.getDbName() + "' does not require identity insert state changes");
        }
        LOG.debug("IDENTITY check SQL: " + sql);
        return sql;
    }

    private boolean requiresIdentityInsert(final String targetTableName, final Connection targetConnection,
            final DataBaseProvider dbProvider, final DataSourceConfiguration dsConfig) {
        if (dbProvider.isPostgreSqlUsed() || dbProvider == DataBaseProvider.MYSQL) {
            return false;
        }

        try (final Statement statement = targetConnection.createStatement();
                final ResultSet resultSet = statement
                        .executeQuery(buildSqlForIdentityInsertCheck(targetTableName, dbProvider, dsConfig))) {
            return resultSet.next() && resultSet.getBoolean(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to check identity insert state", e);
        } catch (UnsupportedOperationException e) {
            LOG.debug("Unsupported identity check: {}", e.getMessage());

            return false;
        }
    }

    private RetriableTask createWriterTask(CopyPipeWriterContext dwc, DataSet dataSet) {
        MigrationContext ctx = dwc.getContext().getMigrationContext();
        if (ctx.isDeletionEnabled()) {
            return new DataDeleteWriterTask(dwc, dataSet);
        } else {
            return new CopyPipeWriterTask(dwc, dataSet);
        }
    }
}
