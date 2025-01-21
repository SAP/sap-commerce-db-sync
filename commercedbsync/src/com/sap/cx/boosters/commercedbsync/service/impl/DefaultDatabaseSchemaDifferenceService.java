/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sap.cx.boosters.commercedbsync.SchemaDifferenceProgress;
import com.sap.cx.boosters.commercedbsync.SchemaDifferenceStatus;
import com.sap.cx.boosters.commercedbsync.TableCandidate;
import com.sap.cx.boosters.commercedbsync.concurrent.MDCTaskDecorator;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.context.SchemaDifferenceContext;
import com.sap.cx.boosters.commercedbsync.filter.DataCopyTableFilter;
import com.sap.cx.boosters.commercedbsync.provider.CopyItemProvider;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseSchemaDifferenceScheduler;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationReportStorageService;
import com.sap.cx.boosters.commercedbsync.service.DatabaseSchemaDifferenceService;
import com.sap.cx.boosters.commercedbsync.service.DatabaseSchemaDifferenceTaskRepository;
import de.hybris.platform.commercedbsynchac.data.SchemaDifferenceResultData;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.SqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultDatabaseSchemaDifferenceService implements DatabaseSchemaDifferenceService {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseSchemaDifferenceService.class);

    private DataCopyTableFilter dataCopyTableFilter;
    private DatabaseMigrationReportStorageService databaseMigrationReportStorageService;
    private CopyItemProvider copyItemProvider;
    private ConfigurationService configurationService;
    private DatabaseSchemaDifferenceTaskRepository taskRepository;
    private DatabaseSchemaDifferenceScheduler databaseSchemaDifferenceScheduler;
    private MDCTaskDecorator mdcTaskDecorator;
    private final List<CompletableFuture<?>> schemaDiffTasks = new ArrayList<>();

    @Override
    public String generateSchemaDifferencesSql(MigrationContext context,
            final SchemaDifferenceResult schemaDifferenceResult) throws Exception {
        final int maxStageMigrations = context.getMaxTargetStagedMigrations();
        final Set<String> stagingPrefixes = findStagingPrefixes(context);
        String schemaSql = "";
        if (stagingPrefixes.size() > maxStageMigrations) {
            final Database databaseModelWithChanges = getDatabaseModelWithChanges4TableDrop(context);
            LOG.info("generateSchemaDifferencesSql..getDatabaseModelWithChanges4TableDrop.. - calibrating changes ");
            schemaSql = context.getDataTargetRepository().asPlatform().getDropTablesSql(databaseModelWithChanges, true);
            LOG.info("generateSchemaDifferencesSql - generated DDL SQLs for DROP. ");
        } else {
            LOG.info(
                    "generateSchemaDifferencesSql..getDatabaseModelWithChanges4TableCreation - calibrating Schema changes ");
            final DatabaseStatus databaseModelWithChanges = getDatabaseModelWithChanges4TableCreation(context,
                    schemaDifferenceResult);
            if (databaseModelWithChanges.isHasSchemaDiff()) {
                LOG.info("generateSchemaDifferencesSql..Schema Diff found - now to generate the SQLs ");

                schemaSql = processChanges(context, schemaDifferenceResult.getTargetSchema().getDatabase(),
                        databaseModelWithChanges.getDatabase());
                schemaSql = postProcess(schemaSql, context);
                LOG.info("generateSchemaDifferencesSql - generated DDL ALTER SQLs. ");
            }

        }

        return schemaSql;
    }

    protected void preProcessDatabaseModel(final MigrationContext migrationContext, final Database model,
            final Set<TableCandidate> tableCandidates) {
        for (Table table : model.getTables()) {
            final Optional<TableCandidate> matchingTableCandidate = tableCandidates.stream()
                    .filter(tableCandidate -> tableCandidate.getFullTableName().equals(table.getName())).findFirst();

            if (matchingTableCandidate.isEmpty()) {
                model.removeTable(table);
            } else {
                final String commonTableName = matchingTableCandidate.get().getCommonTableName();
                if (migrationContext.getExcludedColumns().containsKey(commonTableName)) {
                    migrationContext.getExcludedColumns().get(commonTableName).forEach(col -> {
                        final Column excludedColumn = table.findColumn(col);
                        table.removeColumn(excludedColumn);
                    });
                }
            }
        }
    }

    private String processChanges(final MigrationContext context, final Database currentModel,
            final Database desiredModel) throws IOException {
        final SqlBuilder sqlBuilder = context.getDataTargetRepository().asPlatform().getSqlBuilder();

        try (StringWriter buffer = new StringWriter()) {
            sqlBuilder.setWriter(buffer);
            sqlBuilder.alterDatabase(currentModel, desiredModel, null);
            return buffer.toString();
        }
    }

    /*
     * ORACLE_TARGET - START This a TEMP fix, it is difficlt to get from from Sql
     * Server NVARCHAR(255), NVARCHAR(MAX) to convert properly into to Orcale's
     * VARCHAR2(255) and CLOB respectively. Therefore when the schema script output
     * has VARCHAR2(2147483647) which is from SqlServer's NVARCHAR(max), then we
     * just make it CLOB. Alternatively check if something can be done via the
     * mappings in OracleDataRepository.
     */
    private String postProcess(String schemaSql, final MigrationContext context) {
        if (context.getDataTargetRepository().getDatabaseProvider().isOracleUsed()) {
            schemaSql = schemaSql.replaceAll(CommercedbsyncConstants.MIGRATION_ORACLE_MAX,
                    CommercedbsyncConstants.MIGRATION_ORACLE_VARCHAR24k);
            // another odd character that comes un in the SQL
            LOG.info("Changing the NVARCHAR2 " + schemaSql);
            schemaSql = schemaSql.replaceAll("NUMBER\\(10,0\\) DEFAULT ''''''", "NUMBER(10,0) DEFAULT 0");
        }
        return schemaSql;
    }

    @Override
    public void executeSchemaDifferencesSql(final MigrationContext context, final String sql) throws Exception {

        if (!context.isSchemaMigrationEnabled()) {
            throw new RuntimeException(
                    "Schema migration is disabled. Check property:" + CommercedbsyncConstants.MIGRATION_SCHEMA_ENABLED);
        }

        final Platform platform = context.getDataTargetRepository().asPlatform();
        final boolean continueOnError = false;
        final Connection connection = platform.borrowConnection();
        try {
            platform.evaluateBatch(connection, sql, continueOnError);
            LOG.info("Executed the following sql to change the schema:\n" + sql);
            writeReport(sql);
        } catch (final Exception e) {
            throw new RuntimeException("Could not execute Schema Diff Script", e);
        } finally {
            platform.returnConnection(connection);
        }
    }

    @Override
    public void executeSchemaDifferences(final MigrationContext context) throws Exception {
        final SchemaDifferenceStatus status = startSchemaDifferenceCheckAndWaitForFinish(context);
        executeSchemaDifferencesSql(context, status.getSqlScript());
    }

    private Set<String> findDuplicateTables(final MigrationContext migrationContext) {
        try {
            final Set<String> stagingPrefixes = findStagingPrefixes(migrationContext);
            final Set<String> targetSet = migrationContext.getDataTargetRepository().getAllTableNames();
            return targetSet.stream()
                    .filter(t -> stagingPrefixes.stream().anyMatch(p -> StringUtils.startsWithIgnoreCase(t, p)))
                    .collect(Collectors.toSet());
        } catch (final Exception e) {
            LOG.error("Error occurred while trying to find duplicate tables", e);
        }
        return Collections.emptySet();
    }

    private Set<String> findStagingPrefixes(final MigrationContext context) throws Exception {
        final String currentSystemPrefix = configurationService.getConfiguration().getString("db.tableprefix");
        final String currentMigrationPrefix = context.getDataTargetRepository().getDataSourceConfiguration()
                .getTablePrefix();
        final Set<String> targetSet = context.getDataTargetRepository().getAllTableNames();
        final String deploymentsTable = CommercedbsyncConstants.DEPLOYMENTS_TABLE;
        final Set<String> detectedPrefixes = targetSet.stream().filter(t -> t.toLowerCase().endsWith(deploymentsTable))
                .filter(t -> !StringUtils.equalsIgnoreCase(t, currentSystemPrefix + deploymentsTable))
                .filter(t -> !StringUtils.equalsIgnoreCase(t, currentMigrationPrefix + deploymentsTable))
                .map(t -> StringUtils.removeEndIgnoreCase(t, deploymentsTable)).collect(Collectors.toSet());
        return detectedPrefixes;

    }

    private Database getDatabaseModelWithChanges4TableDrop(final MigrationContext context) {
        final Set<String> duplicateTables = findDuplicateTables(context);
        final Database database = context.getDataTargetRepository().asDatabase(true);
        // clear tables and add only the ones to be removed
        final Table[] tables = database.getTables();
        Stream.of(tables).forEach(t -> {
            database.removeTable(t);
        });
        duplicateTables.forEach(t -> {
            final Table table = ObjectUtils.defaultIfNull(database.findTable(t), new Table());
            table.setName(t);
            database.addTable(table);
        });
        return database;
    }

    protected DatabaseStatus getDatabaseModelWithChanges4TableCreation(final MigrationContext migrationContext,
            final SchemaDifferenceResult differenceResult) throws Exception {
        final DatabaseStatus dbStatus = new DatabaseStatus();

        if (!differenceResult.hasDifferences()) {
            LOG.info("getDatabaseModelWithChanges4TableCreation - No Difference found in schema ");
            dbStatus.setDatabase(migrationContext.getDataTargetRepository().asDatabase());
            dbStatus.setHasSchemaDiff(false);
            return dbStatus;
        }
        final SchemaDifference targetDiff = differenceResult.getTargetSchema();
        final Database database = getDatabaseModelClone(targetDiff.getDatabase());

        // add missing tables in target
        if (migrationContext.isAddMissingTablesToSchemaEnabled()) {
            final List<TableKeyPair> missingTables = targetDiff.getMissingTables();
            for (final TableKeyPair missingTable : missingTables) {
                final Table tableClone = (Table) differenceResult.getSourceSchema().getDatabase()
                        .findTable(missingTable.getLeftName(), false).clone();
                tableClone.setName(missingTable.getRightName());
                tableClone.setCatalog(
                        migrationContext.getDataTargetRepository().getDataSourceConfiguration().getCatalog());
                tableClone
                        .setSchema(migrationContext.getDataTargetRepository().getDataSourceConfiguration().getSchema());
                database.addTable(tableClone);
                LOG.info("getDatabaseModelWithChanges4TableCreation - missingTable.getRightName() ="
                        + missingTable.getRightName() + ", missingTable.getLeftName() = " + missingTable.getLeftName());
            }
        }

        // add missing columns in target
        if (migrationContext.isAddMissingColumnsToSchemaEnabled()) {
            final ListMultimap<TableKeyPair, String> missingColumnsInTable = targetDiff.getMissingColumnsInTable();
            for (final TableKeyPair missingColumnsTable : missingColumnsInTable.keySet()) {
                final List<String> columns = missingColumnsInTable.get(missingColumnsTable);
                for (final String missingColumn : columns) {
                    final Table missingColumnsTableModel = differenceResult.getSourceSchema().getDatabase()
                            .findTable(missingColumnsTable.getLeftName(), false);
                    final Column columnClone = (Column) missingColumnsTableModel.findColumn(missingColumn, false)
                            .clone();
                    LOG.info(" Column " + columnClone.getName() + ", Type = " + columnClone.getType() + ", Type Code "
                            + columnClone.getTypeCode() + ",size " + columnClone.getSize() + ", size as int "
                            + columnClone.getSizeAsInt());
                    // columnClone.set
                    final Table table = database.findTable(missingColumnsTable.getRightName(), false);
                    Preconditions.checkState(table != null, "Data inconsistency: Table must exist.");
                    table.addColumn(columnClone);
                }
            }
        }

        // remove superfluous tables in target
        if (migrationContext.isRemoveMissingTablesToSchemaEnabled()) {
            final SchemaDifference sourceDiff = differenceResult.getSourceSchema();
            final List<TableKeyPair> missingTables = sourceDiff.getMissingTables();
            for (final TableKeyPair missingTable : missingTables) {
                final Table table = database.findTable(missingTable.getLeftName(), false);
                database.removeTable(table);
                LOG.info("getDatabaseModelWithChanges4TableCreation - missingTable.getRightName() ="
                        + missingTable.getRightName() + ", missingTable.getLeftName() = " + missingTable.getLeftName());
            }
        }

        // remove superfluous columns in target
        if (migrationContext.isRemoveMissingColumnsToSchemaEnabled()) {
            final ListMultimap<TableKeyPair, String> superfluousColumnsInTable = differenceResult.getSourceSchema()
                    .getMissingColumnsInTable();
            for (final TableKeyPair superfluousColumnsTable : superfluousColumnsInTable.keySet()) {
                final List<String> columns = superfluousColumnsInTable.get(superfluousColumnsTable);
                for (final String superfluousColumn : columns) {
                    final Table table = database.findTable(superfluousColumnsTable.getLeftName(), false);
                    Preconditions.checkState(table != null, "Data inconsistency: Table must exist.");
                    final Column columnToBeRemoved = table.findColumn(superfluousColumn, false);
                    // remove indices in case column is part of one
                    Stream.of(table.getIndices()).filter(i -> i.hasColumn(columnToBeRemoved))
                            .forEach(i -> table.removeIndex(i));
                    table.removeColumn(columnToBeRemoved);
                }
            }
        }
        dbStatus.setDatabase(database);
        dbStatus.setHasSchemaDiff(true);
        LOG.info("getDatabaseModelWithChanges4TableCreation Schema Diff found -  done ");
        return dbStatus;
    }

    /*
     * Database.clone() does not clone tables properly and adding a new column would
     * result in it being present in both: cloned, and origin db models.
     */
    protected Database getDatabaseModelClone(final Database model) throws CloneNotSupportedException {
        final Database database = new Database();
        database.setName(model.getName());
        database.setIdMethod(model.getIdMethod());
        database.setIdMethod(model.getIdMethod());
        database.setVersion(model.getVersion());
        for (final Table table : model.getTables()) {
            database.addTable((Table) table.clone());
        }

        return database;
    }

    protected void writeReport(final String differenceSql) {
        try {
            final String fileName = String.format("schemaChanges-%s.sql", LocalDateTime.now().getNano());
            databaseMigrationReportStorageService.store(fileName,
                    new ByteArrayInputStream(differenceSql.getBytes(StandardCharsets.UTF_8)));
        } catch (final Exception e) {
            LOG.error("Error executing writing diff report", e);
        }
    }

    @Override
    public SchemaDifferenceResult getSchemaDifferenceFromStatus(final MigrationContext migrationContext,
            final SchemaDifferenceStatus schemaDifferenceStatus) {
        final SchemaDifference sourceSchemaDifference = getSchemaDifferenceFromResult(
                migrationContext.getDataSourceRepository(), schemaDifferenceStatus.getDiffResult().getSource());
        final SchemaDifference targetSchemaDifference = getSchemaDifferenceFromResult(
                migrationContext.getDataTargetRepository(), schemaDifferenceStatus.getDiffResult().getTarget());

        return new SchemaDifferenceResult(sourceSchemaDifference, targetSchemaDifference);
    }

    @Override
    public SchemaDifferenceResult createSchemaDifferenceResult(final MigrationContext migrationContext)
            throws Exception {
        final Set<TableCandidate> sourceTableCandidates = getTables(migrationContext,
                copyItemProvider.getSourceTableCandidates(migrationContext));
        final Set<TableCandidate> targetTableCandidates = getTables(migrationContext,
                copyItemProvider.getTargetTableCandidates(migrationContext));
        final SchemaDifference sourceSchemaDifference = getSchemaDifference(migrationContext, true,
                sourceTableCandidates, targetTableCandidates);
        final SchemaDifference targetSchemaDifference = getSchemaDifference(migrationContext, false,
                sourceTableCandidates, targetTableCandidates);
        return new SchemaDifferenceResult(sourceSchemaDifference, targetSchemaDifference);
    }

    private SchemaDifference getSchemaDifference(final MigrationContext migrationContext,
            final boolean useTargetAsRefDatabase, final Set<TableCandidate> sourceTableCandidates,
            final Set<TableCandidate> targetTableCandidates) throws Exception {
        final DataRepository leftRepository = useTargetAsRefDatabase
                ? migrationContext.getDataTargetRepository()
                : migrationContext.getDataSourceRepository();
        final DataRepository rightRepository = useTargetAsRefDatabase
                ? migrationContext.getDataSourceRepository()
                : migrationContext.getDataTargetRepository();

        LOG.info("computing SCHEMA diff, REF DB = " + leftRepository.getDatabaseProvider().getDbName()
                + " vs Checking in DB = " + rightRepository.getDatabaseProvider().getDbName());

        return useTargetAsRefDatabase
                ? computeDiff(migrationContext, leftRepository, rightRepository, targetTableCandidates,
                        sourceTableCandidates)
                : computeDiff(migrationContext, leftRepository, rightRepository, sourceTableCandidates,
                        targetTableCandidates);
    }

    @Override
    public SchemaDifferenceStatus getSchemaDifferenceStatusById(final String schemaDifferenceId,
            final MigrationContext migrationContext) throws Exception {
        return taskRepository
                .getSchemaDifferenceStatus(new SchemaDifferenceContext(schemaDifferenceId, migrationContext));
    }

    @Override
    public SchemaDifferenceStatus getMostRecentSchemaDifference(final MigrationContext migrationContext)
            throws Exception {
        final String mostRecentSchemaDifferenceId = taskRepository
                .getMostRecentSchemaDifferenceId(new SchemaDifferenceContext(migrationContext));
        if (mostRecentSchemaDifferenceId == null) {
            return null;
        }
        return getSchemaDifferenceStatusById(mostRecentSchemaDifferenceId, migrationContext);
    }

    @Override
    public CompletableFuture<Void> checkSchemaDifferenceAsync(final SchemaDifferenceContext context) {
        final ThreadPoolTaskExecutor executor = createExecutor();

        final CompletableFuture<Boolean> readSourceTask = scheduleReadDatabaseModelTask(context, executor,
                context.getMigrationContext().getDataSourceRepository());
        final CompletableFuture<Boolean> readTargetTask = scheduleReadDatabaseModelTask(context, executor,
                context.getMigrationContext().getDataTargetRepository());
        final CompletableFuture<SchemaDifferenceResult> computeTask = scheduleComputeDiffTask(context, readSourceTask,
                readTargetTask);
        final CompletableFuture<Void> generateSqlTask = scheduleGenerateSqlTask(context, computeTask);

        schemaDiffTasks.add(readSourceTask);
        schemaDiffTasks.add(readTargetTask);
        schemaDiffTasks.add(computeTask);
        schemaDiffTasks.add(generateSqlTask);

        // clear tasks when done
        generateSqlTask.thenRun(schemaDiffTasks::clear);

        executor.shutdown();
        return generateSqlTask;
    }

    @Override
    public void abortRunningSchemaDifference(MigrationContext migrationContext) throws Exception {
        schemaDiffTasks.forEach(task -> task.completeExceptionally(new CancellationException("Aborted!")));
        schemaDiffTasks.clear();

        final SchemaDifferenceStatus status = getMostRecentSchemaDifference(migrationContext);
        if (!status.isCompleted()) {
            databaseSchemaDifferenceScheduler
                    .abort(new SchemaDifferenceContext(status.getSchemaDifferenceId(), migrationContext));
        }
    }

    private ThreadPoolTaskExecutor createExecutor() {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(2);
        executor.setThreadNamePrefix("SchemaDiffWorker-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setTaskDecorator(mdcTaskDecorator);
        executor.initialize();
        return executor;
    }

    private CompletableFuture<Boolean> scheduleReadDatabaseModelTask(final SchemaDifferenceContext context,
            final ThreadPoolTaskExecutor executor, final DataRepository dataRepository) {
        final String profile = dataRepository.getDataSourceConfiguration().getProfile();
        final String pipelinename = String.format("read-%s-db", profile);

        return CompletableFuture.supplyAsync(() -> {
            final Stopwatch timer = Stopwatch.createStarted();
            try (MDC.MDCCloseable ignored = MDC.putCloseable(CommercedbsyncConstants.MDC_PIPELINE, pipelinename)) {
                LOG.info("Reading {} database model ...", profile);
                dataRepository.asDatabase(true);
                final Stopwatch endStop = timer.stop();
                silentlyUpdateCompletedState(context, pipelinename, endStop.toString(), endStop.elapsed().getSeconds(),
                        null);
                return Boolean.TRUE;
            }
        }, executor).exceptionally(e -> {
            LOG.error("Failed to read {} database model", profile, e);
            silentlyUpdateCompletedState(context, pipelinename, StringUtils.EMPTY, 0f, (Exception) e);
            return Boolean.FALSE;
        });
    }

    private CompletableFuture<SchemaDifferenceResult> scheduleComputeDiffTask(final SchemaDifferenceContext context,
            final CompletableFuture<Boolean> readSourceTask, final CompletableFuture<Boolean> readTargetTask) {
        final String pipelinename = "compute-diff";

        return readSourceTask.thenCombine(readTargetTask, (s, t) -> s && t).thenApply(readDbsSuccessful -> {
            if (!readDbsSuccessful) {
                throw new CancellationException("Aborted - previous task failed!");
            }

            final Stopwatch timer = Stopwatch.createStarted();
            try (MDC.MDCCloseable ignored = MDC.putCloseable(CommercedbsyncConstants.MDC_PIPELINE, pipelinename)) {
                final SchemaDifferenceResult schemaDifferenceResult = createSchemaDifferenceResult(
                        context.getMigrationContext());
                LOG.info("Diff finished. Differences detected: {}", schemaDifferenceResult.hasDifferences());
                taskRepository.saveSchemaDifference(context, schemaDifferenceResult.getSourceSchema(), "source");
                taskRepository.saveSchemaDifference(context, schemaDifferenceResult.getTargetSchema(), "target");
                final Stopwatch endStop = timer.stop();
                silentlyUpdateCompletedState(context, pipelinename, endStop.toString(), endStop.elapsed().getSeconds(),
                        null);
                return schemaDifferenceResult;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).exceptionally(e -> {
            LOG.error("Failed to compute diff model", e);
            silentlyUpdateCompletedState(context, pipelinename, StringUtils.EMPTY, 0f, (Exception) e);
            return null;
        });
    }

    private CompletableFuture<Void> scheduleGenerateSqlTask(final SchemaDifferenceContext context,
            final CompletableFuture<SchemaDifferenceResult> computeDiffTask) {
        final String pipelinename = "generate-sql";

        return computeDiffTask.thenAccept(diffResult -> {
            if (diffResult == null) {
                throw new CancellationException("Aborted - previous task failed!");
            }

            try (MDC.MDCCloseable ignored = MDC.putCloseable(CommercedbsyncConstants.MDC_PIPELINE, pipelinename)) {
                final Stopwatch timer = Stopwatch.createStarted();
                final String sqlScript = generateSchemaDifferencesSql(context.getMigrationContext(), diffResult);
                taskRepository.saveSqlScript(context, sqlScript);
                final Stopwatch endStop = timer.stop();
                silentlyUpdateCompletedState(context, pipelinename, endStop.toString(), endStop.elapsed().getSeconds(),
                        null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).whenComplete((r, e) -> {
            if (e != null) {
                LOG.error("Failed to generate SQL script", e);
                silentlyUpdateCompletedState(context, pipelinename, StringUtils.EMPTY, 0, (Exception) e);
            }
        });
    }

    private void silentlyUpdateCompletedState(final SchemaDifferenceContext context, final String pipelinename,
            final String duration, final float durationSeconds, @Nullable final Exception exception) {
        try {
            if (exception == null) {
                taskRepository.markTaskCompleted(context, pipelinename, duration, durationSeconds);
            } else {
                taskRepository.markTaskFailed(context, pipelinename, exception);
            }
        } catch (final Exception e) {
            LOG.error("Failed to update schema diff task status", e);
        }
    }

    @Override
    public String startSchemaDifferenceCheck(final MigrationContext migrationContext) throws Exception {
        return startSchemaDifferenceCheck(migrationContext, false);
    }

    @Override
    public SchemaDifferenceStatus startSchemaDifferenceCheckAndWaitForFinish(final MigrationContext context)
            throws Exception {
        final String schemaDifferenceId = startSchemaDifferenceCheck(context, true);
        final SchemaDifferenceStatus status = getSchemaDifferenceStatusById(schemaDifferenceId, context);

        if (status.isFailed()) {
            throw new Exception("Schema diff failed");
        }

        return status;
    }

    private String startSchemaDifferenceCheck(final MigrationContext migrationContext, final boolean waitForFinish)
            throws Exception {
        final SchemaDifferenceStatus lastSchema = getMostRecentSchemaDifference(migrationContext);
        if (lastSchema != null && lastSchema.getStatus() == SchemaDifferenceProgress.RUNNING) {
            LOG.debug("Found already running schema diff with ID: {}", lastSchema.getSchemaDifferenceId());
            return lastSchema.getSchemaDifferenceId();
        }

        final SchemaDifferenceContext schemaDifferenceContext = new SchemaDifferenceContext(migrationContext);

        try (MDC.MDCCloseable ignored = MDC.putCloseable(CommercedbsyncConstants.MDC_SCHEMADIFFID,
                schemaDifferenceContext.getSchemaDifferenceId())) {
            databaseSchemaDifferenceScheduler.schedule(schemaDifferenceContext);
            LOG.debug("Starting Schema Differences Check with Id {}", schemaDifferenceContext.getSchemaDifferenceId());

            final CompletableFuture<Void> schemaDiffProcess = checkSchemaDifferenceAsync(schemaDifferenceContext);

            if (waitForFinish) {
                schemaDiffProcess.join();
            }
        }

        return schemaDifferenceContext.getSchemaDifferenceId();
    }

    private SchemaDifference getSchemaDifferenceFromResult(final DataRepository dataRepository,
            final SchemaDifferenceResultData schemaDifferenceResultData) {
        SchemaDifference schemaDifference = new SchemaDifference(dataRepository.asDatabase(),
                dataRepository.getDataSourceConfiguration().getTablePrefix());

        for (String[] result : schemaDifferenceResultData.getResults()) {
            final TableKeyPair tableKeyPair = new TableKeyPair(result[0], result[1]);

            if (StringUtils.isNotEmpty(result[2])) {
                for (String missingColumn : result[2].split(taskRepository.MISSING_COLUMN_DELIMITER)) {
                    schemaDifference.getMissingColumnsInTable().put(tableKeyPair, missingColumn);
                }
            } else {
                schemaDifference.missingTables.add(tableKeyPair);
            }
        }

        return schemaDifference;
    }

    protected String getSchemaDifferencesAsJson(final SchemaDifferenceResult schemaDifferenceResult) {
        final Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(schemaDifferenceResult);
    }

    protected SchemaDifference computeDiff(final MigrationContext context, final DataRepository leftRepository,
            final DataRepository rightRepository, final Set<TableCandidate> leftCandidates,
            final Set<TableCandidate> rightCandidates) throws CloneNotSupportedException {
        final Database database = (Database) rightRepository.asDatabase().clone();
        preProcessDatabaseModel(context, database, rightCandidates);
        final SchemaDifference schemaDifference = new SchemaDifference(database,
                rightRepository.getDataSourceConfiguration().getTablePrefix());
        LOG.info("LEFT Repo = " + leftRepository.getDatabaseProvider().getDbName());
        LOG.info("RIGHT Repo = " + rightRepository.getDatabaseProvider().getDbName());

        if (LOG.isDebugEnabled()) {
            try {
                LOG.debug(" All tables in LEFT Repo " + leftRepository.getAllTableNames());
                LOG.debug(" All tables in RIGHT Repo " + rightRepository.getAllTableNames());
            } catch (final Exception e) {
                LOG.error("Cannot fetch all Table Names" + e);
            }
        }

        // LOG.info(" -------------------------------");
        for (final TableCandidate leftCandidate : leftCandidates) {
            LOG.info(" Checking if Left Table exists --> " + leftCandidate.getFullTableName());
            final Table leftTable = leftRepository.asDatabase().findTable(leftCandidate.getFullTableName(), false);
            if (leftTable == null) {
                LOG.error(String.format("Table %s in DB %s cannot be found, but should exist",
                        leftCandidate.getFullTableName(),
                        leftRepository.getDataSourceConfiguration().getConnectionString()));
                continue;

                // throw new RuntimeException(String.format("Table %s in DB %s
                // cannot be found, but should exists",
                // leftCandidate.getFullTableName(),
                // leftRepository.getDataSourceConfiguration().getConnectionString()));
            }
            final String rightTableName = translateTableName(leftRepository, rightRepository, leftCandidate);
            final Table rightTable = rightRepository.asDatabase().findTable(rightTableName, false);
            if (rightTable == null) {
                schemaDifference.getMissingTables().add(new TableKeyPair(leftTable.getName(), rightTableName));
                LOG.info("MISSING Table !! --> " + leftTable.getName() + " searched for " + rightTableName);
            } else {
                // LOG.info(" FOUND Table --> " + rightTable.getName());
                final Column[] leftTableColumns = leftTable.getColumns();
                for (final Column leftTableColumn : leftTableColumns) {
                    if (rightTable.findColumn(leftTableColumn.getName(), false) == null) {
                        LOG.info("Missing column --> " + leftTableColumn.getName() + " -->" + leftTable.getName());
                        schemaDifference.getMissingColumnsInTable().put(
                                new TableKeyPair(leftTable.getName(), rightTable.getName()), leftTableColumn.getName());
                    }
                }
            }
        }
        return schemaDifference;
    }

    private String translateTableName(final DataRepository leftRepository, final DataRepository rightRepository,
            final TableCandidate leftCandidate) {
        String translatedTableName = rightRepository.getDataSourceConfiguration().getTablePrefix()
                + leftCandidate.getBaseTableName();
        if (leftCandidate.isTypeSystemRelatedTable()) {
            translatedTableName += rightRepository.getDataSourceConfiguration().getTypeSystemSuffix();
        }
        // ORCALE_TEMP - START
        /*
         * if (!leftCandidate.getAdditionalSuffix().isEmpty() &&
         * translatedTableName.toLowerCase().endsWith(leftCandidate.
         * getAdditionalSuffix())) {
         * //System.out.println("$$Translated name ends with LP " +
         * translatedTableName); return translatedTableName; }
         */
        // ORCALE_TEMP - END
        return translatedTableName + leftCandidate.getAdditionalSuffix();
    }

    private Set<TableCandidate> getTables(final MigrationContext context, final Set<TableCandidate> candidates) {
        return candidates.stream().filter(c -> dataCopyTableFilter.filter(context).test(c.getCommonTableName()))
                .collect(Collectors.toSet());
    }

    public void setDataCopyTableFilter(final DataCopyTableFilter dataCopyTableFilter) {
        this.dataCopyTableFilter = dataCopyTableFilter;
    }

    public void setDatabaseMigrationReportStorageService(
            final DatabaseMigrationReportStorageService databaseMigrationReportStorageService) {
        this.databaseMigrationReportStorageService = databaseMigrationReportStorageService;
    }

    public void setConfigurationService(final ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setCopyItemProvider(final CopyItemProvider copyItemProvider) {
        this.copyItemProvider = copyItemProvider;
    }

    public void setTaskRepository(final DatabaseSchemaDifferenceTaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    public void setDatabaseSchemaDifferenceScheduler(
            final DatabaseSchemaDifferenceScheduler databaseSchemaDifferenceScheduler) {
        this.databaseSchemaDifferenceScheduler = databaseSchemaDifferenceScheduler;
    }

    public void setMdcTaskDecorator(final MDCTaskDecorator mdcTaskDecorator) {
        this.mdcTaskDecorator = mdcTaskDecorator;
    }

    public static class SchemaDifferenceResult {
        private final SchemaDifference sourceSchema;
        private final SchemaDifference targetSchema;

        public SchemaDifferenceResult(final SchemaDifference sourceSchema, final SchemaDifference targetSchema) {
            this.sourceSchema = sourceSchema;
            this.targetSchema = targetSchema;
        }

        public SchemaDifference getSourceSchema() {
            return sourceSchema;
        }

        public SchemaDifference getTargetSchema() {
            return targetSchema;
        }

        public boolean hasDifferences() {
            final boolean hasMissingTargetTables = getTargetSchema().getMissingTables().size() > 0;
            final boolean hasMissingColumnsInTargetTable = getTargetSchema().getMissingColumnsInTable().size() > 0;
            final boolean hasMissingSourceTables = getSourceSchema().getMissingTables().size() > 0;
            final boolean hasMissingColumnsInSourceTable = getSourceSchema().getMissingColumnsInTable().size() > 0;
            return hasMissingTargetTables || hasMissingColumnsInTargetTable || hasMissingSourceTables
                    || hasMissingColumnsInSourceTable;
        }
    }

    class DatabaseStatus {
        private Database database;

        /**
         * @return the database
         */
        public Database getDatabase() {
            return database;
        }

        /**
         * @param database
         *            the database to set
         */
        public void setDatabase(final Database database) {
            this.database = database;
        }

        /**
         * @return the hasSchemaDiff
         */
        public boolean isHasSchemaDiff() {
            return hasSchemaDiff;
        }

        /**
         * @param hasSchemaDiff
         *            the hasSchemaDiff to set
         */
        public void setHasSchemaDiff(final boolean hasSchemaDiff) {
            this.hasSchemaDiff = hasSchemaDiff;
        }

        private boolean hasSchemaDiff;
    }

    public static class SchemaDifference {

        private final Database database;
        private final String prefix;

        private final List<TableKeyPair> missingTables = new ArrayList<>();
        private final ListMultimap<TableKeyPair, String> missingColumnsInTable = ArrayListMultimap.create();

        public SchemaDifference(final Database database, final String prefix) {
            this.database = database;
            this.prefix = prefix;

        }

        public Database getDatabase() {
            return database;
        }

        public String getPrefix() {
            return prefix;
        }

        public List<TableKeyPair> getMissingTables() {
            return missingTables;
        }

        public ListMultimap<TableKeyPair, String> getMissingColumnsInTable() {
            return missingColumnsInTable;
        }
    }

    public static class TableKeyPair {
        private final String leftName;
        private final String rightName;

        public TableKeyPair(final String leftName, final String rightName) {
            this.leftName = leftName;
            this.rightName = rightName;
        }

        public String getLeftName() {
            return leftName;
        }

        public String getRightName() {
            return rightName;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TableKeyPair that = (TableKeyPair) o;
            return leftName.equals(that.leftName) && rightName.equals(that.rightName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(leftName, rightName);
        }
    }

}
