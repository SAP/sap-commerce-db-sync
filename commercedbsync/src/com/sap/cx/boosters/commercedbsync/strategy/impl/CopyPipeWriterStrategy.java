/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.strategy.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.sap.cx.boosters.commercedbsync.concurrent.DataWorkerExecutor;
import com.sap.cx.boosters.commercedbsync.concurrent.MaybeFinished;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceCategory;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceRecorder;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceUnit;
import com.sap.cx.boosters.commercedbsync.strategy.PipeWriterStrategy;
import de.hybris.bootstrap.ddl.DataBaseProvider;

import java.io.StringReader;
import java.util.Collections;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.concurrent.DataPipe;
import com.sap.cx.boosters.commercedbsync.concurrent.DataWorkerPoolFactory;
import com.sap.cx.boosters.commercedbsync.concurrent.RetriableTask;
import com.sap.cx.boosters.commercedbsync.concurrent.impl.DefaultDataWorkerExecutor;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataColumn;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.dataset.impl.DefaultDataSet;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class CopyPipeWriterStrategy implements PipeWriterStrategy<DataSet> {
    private static final Logger LOG = LoggerFactory.getLogger(CopyPipeWriterStrategy.class);

    private final DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService;

    private final DatabaseCopyTaskRepository taskRepository;

    private final DataWorkerPoolFactory dataWriteWorkerPoolFactory;

    private static final String LP_SUFFIX = "lp";

    public CopyPipeWriterStrategy(DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService, DatabaseCopyTaskRepository taskRepository, DataWorkerPoolFactory dataWriteWorkerPoolFactory) {
        this.databaseMigrationDataTypeMapperService = databaseMigrationDataTypeMapperService;
        this.taskRepository = taskRepository;
        this.dataWriteWorkerPoolFactory = dataWriteWorkerPoolFactory;
    }

    @Override
    public void write(CopyContext context, DataPipe<DataSet> pipe, CopyContext.DataCopyItem item) throws Exception {
		// ORACLE_TARGET - START
		// Fetch the provider to figure out the name of the DBName
		final DataBaseProvider dbProvider = context.getMigrationContext().getDataTargetRepository()
				.getDatabaseProvider();
		// ORACLE_TARGET - END
        String targetTableName = item.getTargetItem();
        PerformanceRecorder performanceRecorder = context.getPerformanceProfiler().createRecorder(PerformanceCategory.DB_WRITE, targetTableName);
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
             ResultSet metaResult = stmt.executeQuery(String.format("select * from %s where 0 = 1", item.getSourceItem()));
        ) {
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
            throw new IllegalStateException(String.format("%s: source has no columns or all columns excluded", item.getPipelineName()));
        }
        ThreadPoolTaskExecutor taskExecutor = dataWriteWorkerPoolFactory.create(context);
        DataWorkerExecutor<Boolean> workerExecutor = new DefaultDataWorkerExecutor<>(taskExecutor);
        Connection targetConnection = null;
        AtomicLong totalCount = new AtomicLong(0);
        List<String> upsertIds = new ArrayList<>();
        try {
            targetConnection = context.getMigrationContext().getDataTargetRepository().getConnection();
			// ORACLE_TARGET - START - pass the dbProvider and dsConfiguration
			// information into the requiredidentityinsert function

            boolean requiresIdentityInsert = false;
            if (dbProvider.isPostgreSqlUsed()){
                // do nothing
            } else {
                requiresIdentityInsert = requiresIdentityInsert(item.getTargetItem(), targetConnection,
                        dbProvider, context.getMigrationContext().getDataTargetRepository().getDataSourceConfiguration());
            }
			// ORACLE_TARGET - START - pass the dbProvider info into the
			// requiredidentityinsert function
            MaybeFinished<DataSet> sourcePage;
            boolean firstPage = true;
            do {
                sourcePage = pipe.get();
                if (sourcePage.isPoison()) {
                    throw new IllegalStateException("Poison received; dying. Check the logs for further insights.");
                }
                DataSet dataSet = sourcePage.getValue();
                if (firstPage) {
                    doTruncateIfNecessary(context, item.getTargetItem());
                    doTurnOnOffIndicesIfNecessary(context, item.getTargetItem(), false);
                    if (context.getMigrationContext().isIncrementalModeEnabled()) {
                        if (context.getMigrationContext().isLpTableMigrationEnabled()
                            && StringUtils.endsWithIgnoreCase(item.getSourceItem(),LP_SUFFIX)){
                           determineLpUpsertId(upsertIds, dataSet);
                        } else{
                           determineUpsertId(upsertIds, dataSet);
                        }
                    }
                    firstPage = false;
                }
                if (dataSet.isNotEmpty()) {
                    DataWriterContext dataWriterContext = new DataWriterContext(context, item, dataSet, columnsToCopy, nullifyColumns, performanceRecorder, totalCount, upsertIds, requiresIdentityInsert);
                    RetriableTask writerTask = createWriterTask(dataWriterContext);
                    workerExecutor.safelyExecute(writerTask);
                }
            } while (!sourcePage.isDone());
            workerExecutor.waitAndRethrowUncaughtExceptions();
            if (taskExecutor != null) {
                taskExecutor.shutdown();
            }
        } catch (Exception e) {
            pipe.requestAbort(e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        } finally {
            if (targetConnection != null) {
                doTurnOnOffIndicesIfNecessary(context, item.getTargetItem(), true);
                targetConnection.close();
            }
            updateProgress(context, item, totalCount.get());
        }
    }

    private void switchIdentityInsert(Connection connection, final String tableName, boolean on) {
        try (Statement stmt = connection.createStatement()) {
            String onOff = on ? "ON" : "OFF";
            stmt.executeUpdate(String.format("SET IDENTITY_INSERT %s %s", tableName, onOff));
        } catch (final Exception e) {
            //TODO using brute force FIX
        }
    }

    protected void executeBatch(CopyContext.DataCopyItem item, PreparedStatement preparedStatement, long batchCount, PerformanceRecorder recorder) throws SQLException {
        final Stopwatch timer = Stopwatch.createStarted();
        preparedStatement.executeBatch();
        preparedStatement.clearBatch();
        LOG.debug("Batch written ({} items) for table '{}' in {}", batchCount, item.getTargetItem(), timer.stop().toString());
        recorder.record(PerformanceUnit.ROWS, batchCount);
    }

    private void updateProgress(CopyContext context, CopyContext.DataCopyItem item, long totalCount) {
        try {
            taskRepository.updateTaskProgress(context, item, totalCount);
        } catch (Exception e) {
            LOG.warn("Could not update progress", e);
        }
    }

    protected void doTruncateIfNecessary(CopyContext context, String targetTableName) throws Exception {
        if (context.getMigrationContext().isTruncateEnabled()) {
            if (!context.getMigrationContext().getTruncateExcludedTables().contains(targetTableName)) {
                assertTruncateAllowed(context, targetTableName);
                context.getMigrationContext().getDataTargetRepository().truncateTable(targetTableName);
            }
        }
    }

    protected void doTurnOnOffIndicesIfNecessary(CopyContext context, String targetTableName, boolean on) throws Exception {
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

    protected void assertTruncateAllowed(CopyContext context, String targetTableName) throws Exception {
        if (context.getMigrationContext().isIncrementalModeEnabled()) {
            throw new IllegalStateException("Truncating tables in incremental mode is illegal. Change the property " + CommercedbsyncConstants.MIGRATION_DATA_TRUNCATE_ENABLED + " to false");
        }
    }

    protected boolean isColumnOverride(CopyContext context, CopyContext.DataCopyItem item, String sourceColumnName) {
        return MapUtils.isNotEmpty(item.getColumnMap()) && item.getColumnMap().containsKey(sourceColumnName);
    }

    protected boolean isColumnOverride(CopyContext context, CopyContext.DataCopyItem item) {
        return MapUtils.isNotEmpty(item.getColumnMap());
    }

	private PreparedStatement createPreparedStatement(final CopyContext context, final String targetTableName,
			final List<String> columnsToCopy, final List<String> upsertIds, final Connection targetConnection)
			throws Exception {
		if (context.getMigrationContext().isIncrementalModeEnabled()) {
			if (!upsertIds.isEmpty()) {
				// ORACLE_TARGET - START
				String sqlBuild = "";
				if (context.getMigrationContext().getDataTargetRepository().getDatabaseProvider().isOracleUsed()) {
					sqlBuild = getBulkUpsertStatementOracle(targetTableName, columnsToCopy, upsertIds.get(0));
				} else if (context.getMigrationContext().getDataTargetRepository().getDatabaseProvider().isHanaUsed()) {
                    sqlBuild = getBulkUpsertStatementHana(targetTableName, columnsToCopy, upsertIds);
                } else if (context.getMigrationContext().getDataTargetRepository().getDatabaseProvider().isPostgreSqlUsed()) {
                    sqlBuild = getBulkUpsertStatementPostGres(targetTableName, columnsToCopy, upsertIds.get(0));
                }
                else {
					sqlBuild = getBulkUpsertStatement(targetTableName, columnsToCopy, upsertIds);
				}
				return targetConnection.prepareStatement(sqlBuild);
				// ORACLE_TARGET - END
			} else {
				throw new RuntimeException(
						"The incremental approach can only be used on tables that have a valid identifier like PK or ID");
			}
		} else {
			return targetConnection.prepareStatement(getBulkInsertStatement(targetTableName, columnsToCopy,
					columnsToCopy.stream().map(column -> "?").collect(Collectors.toList())));
		}
	}

    private String getBulkInsertStatement(String targetTableName, List<String> columnsToCopy, List<String> columnsToCopyValues) {
        return "INSERT INTO " + targetTableName + " " + getBulkInsertStatementParamList(columnsToCopy, columnsToCopyValues);
    }

    private String getBulkInsertStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues) {
        return "("
                + String.join(", ", columnsToCopy) + ") VALUES ("
                + columnsToCopyValues.stream().collect(Collectors.joining(", "))
                + ")";
    }

    private String getBulkUpdateStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues) {
        return "SET " + IntStream.range(0, columnsToCopy.size()).mapToObj(idx -> String.format("%s = %s", columnsToCopy.get(idx), columnsToCopyValues.get(idx))).collect(Collectors.joining(", "));
    }

	// ORACLE_TARGET -- START
	private String getBulkUpdateStatementParamListOracle(final List<String> columnsToCopy,
			final List<String> columnsToCopyValues) {

		final List<String> columnsToCopyMinusPK = columnsToCopy.stream().filter(s -> !s.equalsIgnoreCase("PK"))
				.collect(Collectors.toList());
		final List<String> columnsToCopyValuesMinusPK = columnsToCopyValues.stream()
				.filter(s -> !s.equalsIgnoreCase("s.PK")).collect(Collectors.toList());
		LOG.debug("getBulkUpdateStatementParamListOracle - columnsToCopyMinusPK =" + columnsToCopyMinusPK);
		return "SET " + IntStream.range(0, columnsToCopyMinusPK.size()).mapToObj(
				idx -> String.format("%s = %s", columnsToCopyMinusPK.get(idx), columnsToCopyValuesMinusPK.get(idx)))
				.collect(Collectors.joining(", "));
	}
	// ORACLE_TARGET -- END
    private void determineUpsertId(List<String> upsertIds ,DataSet dataSet) {
        if (dataSet.hasColumn("PK")) {
            upsertIds.add("PK");
            return;
        } else if (dataSet.hasColumn("ID")) {
            upsertIds.add("ID");
            return;
        } else {
            //should we support more IDs? In the hybris context there is hardly any other with regards to transactional data.
            return ;
        }
    }

    private void determineLpUpsertId(List<String> upsertIds ,DataSet dataSet) {
        if (dataSet.hasColumn("ITEMPK")
            && dataSet.hasColumn("LANGPK")) {
            upsertIds.add("ITEMPK");
            upsertIds.add("LANGPK");
            return;
        } else{
            //should we support more IDs? In the hybris context there is hardly any other with regards to transactional data.
            return;
        }
    }

    private String getBulkUpsertStatement(String targetTableName, List<String> columnsToCopy, List<String> upsertIds) {
        /*
         * https://michaeljswart.com/2017/07/sql-server-upsert-patterns-and-antipatterns/
         * We are not using a stored procedure here as CCv2 does not grant sp exec permission to the default db user
         */
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("MERGE %s WITH (HOLDLOCK) AS t", targetTableName));
        sqlBuilder.append("\n");
        sqlBuilder.append(String.format("USING (SELECT %s) AS s ON ", Joiner.on(',').join(columnsToCopy.stream().map(column -> "? " + column).collect(Collectors.toList()))));
        sqlBuilder.append(String.format("( %s )" , upsertIds.stream().map(column -> String.format(" t.%s = s.%s",column,column)).collect(Collectors.joining(" AND "))));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN MATCHED THEN UPDATE"); //update
        sqlBuilder.append("\n");
        sqlBuilder.append(getBulkUpdateStatementParamList(columnsToCopy, columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN NOT MATCHED THEN INSERT"); //insert
        sqlBuilder.append("\n");
        sqlBuilder.append(getBulkInsertStatementParamList(columnsToCopy, columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
        sqlBuilder.append(";");
		// ORACLE_TARGET
		LOG.debug("UPSERT SQL SERVER SQl builder=" + sqlBuilder.toString());
		return sqlBuilder.toString();
	}

	// ORACLE_TARGET - START
	private String getBulkUpsertStatementOracle(final String targetTableName, final List<String> columnsToCopy,
			final String columnId) {

		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append(String.format("MERGE INTO %s t", targetTableName));
		sqlBuilder.append("\n");
		sqlBuilder.append(String.format("USING (SELECT %s from dual) s ON (t.%s = s.%s)",
				Joiner.on(',').join(columnsToCopy.stream().map(column -> "? " + column).collect(Collectors.toList())),
				columnId, columnId));
		sqlBuilder.append("\n");
		sqlBuilder.append("WHEN MATCHED THEN UPDATE"); // update
		sqlBuilder.append("\n");
		sqlBuilder.append(getBulkUpdateStatementParamListOracle(columnsToCopy,
				columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
		sqlBuilder.append("\n");
		sqlBuilder.append("WHEN NOT MATCHED THEN INSERT"); // insert
		sqlBuilder.append("\n");
		sqlBuilder.append(getBulkInsertStatementParamList(columnsToCopy,
				columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
		// sqlBuilder.append(";");
		// ORACLE_TARGET
		LOG.debug("UPSERT ORACLE SQl builder=" + sqlBuilder.toString());
		return sqlBuilder.toString();
	}
	// ORACLE_TARGET - END

    private String getBulkUpsertStatementPostGres(final String targetTableName, final List<String> columnsToCopy,
                                                final String columnId) {

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("MERGE INTO %s t", targetTableName));
        sqlBuilder.append("\n");
        sqlBuilder.append(String.format("USING (SELECT %s from dual) s ON (t.%s = s.%s)",
                Joiner.on(',').join(columnsToCopy.stream().map(column -> "? " + column).collect(Collectors.toList())),
                columnId, columnId));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN MATCHED THEN UPDATE"); // update
        sqlBuilder.append("\n");
        sqlBuilder.append(getBulkUpdateStatementParamListOracle(columnsToCopy,
                columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN NOT MATCHED THEN INSERT"); // insert
        sqlBuilder.append("\n");
        sqlBuilder.append(getBulkInsertStatementParamList(columnsToCopy,
                columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
        // sqlBuilder.append(";");
        // ORACLE_TARGET
        LOG.debug("UPSERT PostGres SQl builder=" + sqlBuilder.toString());
        return sqlBuilder.toString();
    }

    private String getBulkUpsertStatementHana(final String targetTableName, final List<String> columnsToCopy,
                                              List<String> upsertIds) {
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("MERGE INTO %s t", targetTableName));
        sqlBuilder.append("\n");
        sqlBuilder.append(String.format("USING (SELECT %s from dummy) s ON ", Joiner.on(',').join(columnsToCopy.stream().map(column -> "? " + column).collect(Collectors.toList()))));
        sqlBuilder.append(String.format("( %s )" , upsertIds.stream().map(column -> String.format(" t.%s = s.%s",column,column)).collect(Collectors.joining(" AND "))));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN MATCHED THEN UPDATE"); // update
        sqlBuilder.append("\n");
        sqlBuilder.append(getBulkUpdateStatementParamListOracle(columnsToCopy,
                columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN NOT MATCHED THEN INSERT"); // insert
        sqlBuilder.append("\n");
        sqlBuilder.append(getBulkInsertStatementParamList(columnsToCopy,
                columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
        // sqlBuilder.append(";");
        // ORACLE_TARGET
        LOG.debug("UPSERT HANA SQl builder=" + sqlBuilder.toString());
        return sqlBuilder.toString();
    }

    private String getBulkDeleteStatement(String targetTableName, String columnId) {
        /*
         * https://michaeljswart.com/2017/07/sql-server-upsert-patterns-and-antipatterns/
         * We are not using a stored procedure here as CCv2 does not grant sp exec permission to the default db user
         */
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("MERGE %s WITH (HOLDLOCK) AS t", targetTableName));
        sqlBuilder.append("\n");
        sqlBuilder.append(String.format("USING (SELECT %s) AS s ON t.%s = s.%s",  "? " + columnId, columnId, columnId));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN MATCHED THEN DELETE"); //DELETE
        sqlBuilder.append(";");
		// ORACLE_TARGET
		LOG.debug("MERGE-DELETE SQL Server " + sqlBuilder.toString());
        return sqlBuilder.toString();
    }

    // ORACLE_TARGET - START
    private String getBulkDeleteStatementOracle(final String targetTableName, final String columnId) {
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("MERGE INTO  %s  t", targetTableName));
        sqlBuilder.append("\n");
        // sqlBuilder.append(String.format("USING (SELECT %s , '2022-02-15
        // 10:48:49.496' modifiedTS from dual) s ON (t.%s = s.%s)",
        // "? " + columnId, columnId, columnId));
        sqlBuilder.append(
                String.format("USING (SELECT %s from dual) s ON (t.%s = s.%s)", "? " + columnId, columnId, columnId));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN MATCHED THEN "); // DELETE
        sqlBuilder.append("UPDATE SET t.HJMPTS = 0 "); // IS INSERT OR UPDATE
        // MANDATORY, therefore
        // setting a dummy
        // value. Hopefully
        // HJMPTS is present in
        // all tables
        sqlBuilder.append("DELETE WHERE " + String.format(" t.%s = s.%s ", columnId, columnId));// DELETE
        // is
        // OPTIONAL
        // sqlBuilder.append(";");
        // ORACLE_TARGET
        LOG.debug("MERGE-DELETE ORACLE " + sqlBuilder.toString());
        return sqlBuilder.toString();
    }
    // ORACLE_TARGET - END

    // ORACLE_TARGET -- START Helper Function 1
    private StringBuilder buildSqlForIdentityInsertCheck(final String targetTableName,
                                                         final DataBaseProvider dbProvider, final DataSourceConfiguration dsConfig) {
        final StringBuilder sqlBuilder = new StringBuilder();
        if (dbProvider.isMssqlUsed()) {
            sqlBuilder.append("SELECT \n");
            sqlBuilder.append("count(*)\n");
            sqlBuilder.append("FROM sys.columns\n");
            sqlBuilder.append("WHERE\n");
            sqlBuilder.append(String.format("object_id = object_id('%s')\n", targetTableName));
            sqlBuilder.append("AND\n");
            sqlBuilder.append("is_identity = 1\n");
            sqlBuilder.append(";\n");
        } else if (dbProvider.isOracleUsed()) {
            // get schema name
            final String schema = dsConfig.getSchema();
            sqlBuilder.append("SELECT \n");
            sqlBuilder.append("has_identity\n");
            sqlBuilder.append("FROM dba_tables\n");
            sqlBuilder.append("WHERE\n");
            sqlBuilder.append(String.format("UPPER(table_name) = UPPER('%s')\n", targetTableName));
            sqlBuilder.append(String.format(" AND UPPER(owner) = UPPER('%s')\n", schema));
            // sqlBuilder.append(";\n");
        } else if (dbProvider.isHanaUsed()) {
            // get schema name
            final String schema = dsConfig.getSchema();
            sqlBuilder.append("SELECT \n");
            sqlBuilder.append("is_insert_only\n");
            sqlBuilder.append("FROM public.tables\n");
            sqlBuilder.append("WHERE\n");
            sqlBuilder.append(String.format("table_name = UPPER('%s')\n", targetTableName));
            sqlBuilder.append(String.format(" AND schema_name = UPPER('%s')\n", schema));
            // sqlBuilder.append(";\n");
        }
        else {
            sqlBuilder.append("SELECT \n");
            sqlBuilder.append("count(*)\n");
            sqlBuilder.append("FROM sys.columns\n");
            sqlBuilder.append("WHERE\n");
            sqlBuilder.append(String.format("object_id = object_id('%s')\n", targetTableName));
            sqlBuilder.append("AND\n");
            sqlBuilder.append("is_identity = 1\n");
            sqlBuilder.append(";\n");
        }
        LOG.debug("IDENTITY check SQL -> " + sqlBuilder);
        return sqlBuilder;
    }
    // ORACLE_TARGET -- END

    // ORACLE_TARGET -- START Helper Function 2
    private boolean checkIdentityfromResultSet(final ResultSet resultSet, final DataBaseProvider dbProvider)
            throws SQLException {
        boolean requiresIdentityInsert = false;

        final String dbName = dbProvider.getDbName().toLowerCase();
        if (resultSet.next()) {
            if (dbProvider.isMssqlUsed()) {
                requiresIdentityInsert = resultSet.getInt(1) > 0;
            } else if (dbProvider.isOracleUsed()) {
                requiresIdentityInsert = resultSet.getBoolean(1);
            }  else if (dbProvider.isHanaUsed()) {
                requiresIdentityInsert = resultSet.getBoolean(1);
            } else{
                requiresIdentityInsert = resultSet.getInt(1) > 0;
            }
        }
        return requiresIdentityInsert;

    }
    // ORACLE_TARGET -- END

    // ORACLE_TARGET -- START
    private boolean requiresIdentityInsert(final String targetTableName, final Connection targetConnection,
                                           final DataBaseProvider dbProvider, final DataSourceConfiguration dsConfig) {
        final StringBuilder sqlBuilder = buildSqlForIdentityInsertCheck(targetTableName, dbProvider, dsConfig);

        try (
            final Statement statement = targetConnection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
        ){
            final boolean requiresIdentityInsert = checkIdentityfromResultSet(resultSet, dbProvider);

            return requiresIdentityInsert;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    // ORACLE_TARGET -- END

    private boolean requiresIdentityInsert(String targetTableName, Connection targetConnection) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT \n");
        sqlBuilder.append("count(*)\n");
        sqlBuilder.append("FROM sys.columns\n");
        sqlBuilder.append("WHERE\n");
        sqlBuilder.append(String.format("object_id = object_id('%s')\n", targetTableName));
        sqlBuilder.append("AND\n");
        sqlBuilder.append("is_identity = 1\n");
        sqlBuilder.append(";\n");
        try (
            Statement statement = targetConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
        ) {
            boolean requiresIdentityInsert = false;
            if (resultSet.next()) {
                requiresIdentityInsert = resultSet.getInt(1) > 0;
            }
            return requiresIdentityInsert;
        }
        catch (SQLException e) {
        throw new RuntimeException(e);
       }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private RetriableTask createWriterTask(DataWriterContext dwc) {
        MigrationContext ctx = dwc.getContext().getMigrationContext();
        if(ctx.isDeletionEnabled()){
          return new DataDeleteWriterTask(dwc);
        } else {

            if (!ctx.isBulkCopyEnabled()) {
                return new DataWriterTask(dwc);
            } else {
                boolean noNullification = dwc.getNullifyColumns().isEmpty();
                boolean noIncremental = !ctx.isIncrementalModeEnabled();
                boolean noColumnOverride = !isColumnOverride(dwc.getContext(), dwc.getCopyItem());
                if (noNullification && noIncremental && noColumnOverride) {
                    LOG.warn("EXPERIMENTAL: Using bulk copy for {}",
                        dwc.getCopyItem().getTargetItem());
                    return new DataBulkWriterTask(dwc);
                } else {
                    return new DataWriterTask(dwc);
                }
            }
        }
    }

    private static class DataWriterContext {
        private CopyContext context;
        private CopyContext.DataCopyItem copyItem;
        private DataSet dataSet;
        private List<String> columnsToCopy;
        private Set<String> nullifyColumns;
        private PerformanceRecorder performanceRecorder;
        private AtomicLong totalCount;
        private List<String> upsertIds;
        private boolean requiresIdentityInsert;

        public DataWriterContext(CopyContext context, CopyContext.DataCopyItem copyItem, DataSet dataSet, List<String> columnsToCopy, Set<String> nullifyColumns, PerformanceRecorder performanceRecorder, AtomicLong totalCount, List<String> upsertIds, boolean requiresIdentityInsert) {
            this.context = context;
            this.copyItem = copyItem;
            this.dataSet = dataSet;
            this.columnsToCopy = columnsToCopy;
            this.nullifyColumns = nullifyColumns;
            this.performanceRecorder = performanceRecorder;
            this.totalCount = totalCount;
            this.upsertIds = upsertIds;
            this.requiresIdentityInsert = requiresIdentityInsert;
        }

        public CopyContext getContext() {
            return context;
        }

        public CopyContext.DataCopyItem getCopyItem() {
            return copyItem;
        }

        public DataSet getDataSet() {
            return dataSet;
        }

        public List<String> getColumnsToCopy() {
            return columnsToCopy;
        }

        public Set<String> getNullifyColumns() {
            return nullifyColumns;
        }

        public PerformanceRecorder getPerformanceRecorder() {
            return performanceRecorder;
        }

        public AtomicLong getTotalCount() {
            return totalCount;
        }

        public List<String> getUpsertId() {
            return upsertIds;
        }

        public boolean isRequiresIdentityInsert() {
            return requiresIdentityInsert;
        }
    }

    private class DataWriterTask extends RetriableTask {

        private DataWriterContext ctx;

        public DataWriterTask(DataWriterContext ctx) {
            super(ctx.getContext(), ctx.getCopyItem().getTargetItem());
            this.ctx = ctx;
        }

        @Override
        protected Boolean internalRun() {
            try {
                if (!ctx.getDataSet().getAllResults().isEmpty()) {
                        process();
                }
                return Boolean.TRUE;
            } catch (Exception e) {
                //LOG.error("Error while executing table task " + ctx.getCopyItem().getTargetItem(),e);
                throw new RuntimeException("Error processing writer task for " + ctx.getCopyItem().getTargetItem(), e);
            }
        }

        private void process() throws Exception {
            Connection connection = null;
            Boolean originalAutoCommit = null;
            boolean requiresIdentityInsert = ctx.isRequiresIdentityInsert();
            try {
                connection = ctx.getContext().getMigrationContext().getDataTargetRepository().getConnection();
				// ORACLE_TARGET - START Fetch the provider to figure out the
				// name of the DBName
				final DataBaseProvider dbProvider = ctx.getContext().getMigrationContext().getDataTargetRepository()
						.getDatabaseProvider();
				LOG.debug("TARGET DB name = " + dbProvider.getDbName() + " SOURCE TABLE = " + ctx.getCopyItem().getSourceItem()
						+ ", TARGET Table = " + ctx.getCopyItem().getTargetItem());
				/*
				 * if
				 * (ctx.getCopyItem().getTargetItem().equalsIgnoreCase("medias")
				 * ) { return; }
				 */
				// ORACLE_TARGET - END Fetch the provider to figure out the name
				// of the DBName
                originalAutoCommit = connection.getAutoCommit();
                try (PreparedStatement bulkWriterStatement = createPreparedStatement(ctx.getContext(), ctx.getCopyItem().getTargetItem(), ctx.getColumnsToCopy(), ctx.getUpsertId(), connection);
                     Statement tempStmt = connection.createStatement();
                     ResultSet tempTargetRs = tempStmt.executeQuery(String.format("select * from %s where 0 = 1", ctx.getCopyItem().getTargetItem()))) {
                    connection.setAutoCommit(false);
                    if (requiresIdentityInsert) {
                        switchIdentityInsert(connection, ctx.getCopyItem().getTargetItem(), true);
                    }
					// ORACLE_TARGET - START - just to print once, helpful to
					// debug issues at the time of actual copy.
					boolean printed2004 = false;
					boolean printed2005 = false;
					final boolean printedDef = false;
					// ORACLE_TARGET - END - just to print once, helpful to
					// debug issues at the time of actual copy.
                    for (List<Object> row : ctx.getDataSet().getAllResults()) {
                        int sourceColumnTypeIdx = 0;
                        int paramIdx = 1;
                        for (String sourceColumnName : ctx.getColumnsToCopy()) {
                            int targetColumnIdx = tempTargetRs.findColumn(sourceColumnName);
                            DataColumn sourceColumnType = ((DefaultDataSet) ctx.getDataSet()).getColumnOrder().get(sourceColumnTypeIdx);
                            int targetColumnType = tempTargetRs.getMetaData().getColumnType(targetColumnIdx);
                            if (ctx.getNullifyColumns().contains(sourceColumnName)) {
                                bulkWriterStatement.setNull(paramIdx, targetColumnType);
                                LOG.trace("Column {} is nullified. Setting NULL value...", sourceColumnName);
                            } else {
                                if (isColumnOverride(ctx.getContext(), ctx.getCopyItem(), sourceColumnName)) {
                                    bulkWriterStatement.setObject(paramIdx, ctx.getCopyItem().getColumnMap().get(sourceColumnName), targetColumnType);
                                } else {
                                    Object sourceColumnValue = null;
                                    if(dbProvider.isPostgreSqlUsed()){
                                         sourceColumnValue = ctx.getDataSet().getColumnValueForPostGres(sourceColumnName, row,sourceColumnType,targetColumnType);
                                    }
                                    else if(dbProvider.isHanaUsed()){
                                        sourceColumnValue = ((DefaultDataSet) ctx.getDataSet()).getColumnValueForHANA(sourceColumnName, row,sourceColumnType,targetColumnType);
                                    }
                                    else{
                                        sourceColumnValue = ctx.getDataSet().getColumnValue(sourceColumnName, row);
                                    }
                                    if (sourceColumnValue != null) {
										// ##ORACLE_TARGET -- START TRY-catch to
										// catch all exceptions, not to print
										// each time, print one for each
										// type/worker.
										try {
											if (! dbProvider.isOracleUsed()) {
												// for all cases non-oracle
												bulkWriterStatement.setObject(paramIdx, sourceColumnValue,
														targetColumnType);
											} else {
												// if type is oracle, then there
												// are a bunch of exceptions
												// when the type is 2004, 2005
												// 2004 = BLOB , 2005 = CLOB
												switch (targetColumnType) {
												/*
												 * code to handle BLOB, because
												 * setObject throws exception
												 * example Products.p_buyerids
												 * is varbinary(max) in
												 * (sqlserver) AND blob in
												 * (oracle)
												 */
												// TODO Use Constant definitions
												case 2004: {
													// temp debug code - start
													// ....only to print once..
													if (!printed2004) {
														LOG.debug("BLOB 2004 sourceColumnName = " + sourceColumnName
																+ " souce value type CN="
																+ sourceColumnValue.getClass().getCanonicalName()
																+ " , Name = " + sourceColumnValue.getClass().getName()
																+ " , Type Name = "
																+ sourceColumnValue.getClass().getTypeName());
														printed2004 = true;
													}
													// temp debug code end
													bulkWriterStatement.setBytes(paramIdx, (byte[]) sourceColumnValue);
													break;

												}
												/*
												 * code to handle CLOB, because
												 * setObject throws exception
												 * example Promotion.description
												 * is nvarchar(max) in
												 * (sqlserver) AND blob in
												 * (oracle)
												 */
												case 2005: {
													// temp debug code - start
													// ....only to print once..
													if (!printed2005) {
														LOG.debug("CLOB 2005 sourceColumnName = " + sourceColumnName
																+ " souce value type CN="
																+ sourceColumnValue.getClass().getCanonicalName()
																+ " , Name = " + sourceColumnValue.getClass().getName()
																+ " , Type Name = "
																+ sourceColumnValue.getClass().getTypeName());
														printed2005 = true;
													}
													// temp debug code end
													// CLOB or NCLOB ?? String
													// -> StringReader
													// bulkWriterStatement.setBytes(paramIdx,
													// (byte[])
													// sourceColumnValue);
													// bulkWriterStatement.setClob(paramIdx,
													// (Clob)
													// sourceColumnValue);
													if (sourceColumnValue instanceof java.lang.String) {
														final String clobString = (String) sourceColumnValue;
														// typically a
														// StringReader is
														// enough, but exception
														// occurs when the value
														// is empty...therefore
														// set to null
														if (!clobString.isEmpty()) {
															LOG.debug(" reading CLOB");
															// LOG.info("CLOB is
															// not empty");
															bulkWriterStatement.setClob(paramIdx,
																	new StringReader((String) sourceColumnValue),
																	((String) sourceColumnValue).length());
															LOG.debug(" wrote CLOB");
														} else {
															LOG.debug("CLOB is  empty...setting null");
															bulkWriterStatement.setNull(paramIdx, targetColumnType);
														}
													}
													break;
												}
												default: {
													bulkWriterStatement.setObject(paramIdx, sourceColumnValue,
															targetColumnType);
													break;
												}
												}

											}
										} catch (final NumberFormatException e) {
											/*
											 * To handle SqlServer CHAR ->
											 * Oracle Number. example
											 * Medias.p_fieldseparator
											 */
											LOG.error(
													"NumberFormatException - Error setting Type on sourceColumnName = "
															+ sourceColumnName + ", sourceColumnValue = "
															+ sourceColumnValue + ", targetColumnType ="
															+ targetColumnType + ", source type = "
															+ sourceColumnValue.getClass().getTypeName());
											if (dbProvider.isOracleUsed()) {
												if (sourceColumnValue instanceof java.lang.String) {
													final char character = sourceColumnValue.toString().charAt(0);
													final int ascii = character;
													// 2 is NUMBER..need to use
													// constants NOW
													if (targetColumnType == 2) {
														// bulkWriterStatement.setIn(paramIdx,
														// ascii,
														// targetColumnType);
														bulkWriterStatement.setInt(paramIdx, ascii);
													}
												}
											}
										} catch (final Exception e) {
											LOG.error("Error setting Type on sourceColumnName = " + sourceColumnName
													+ ", sourceColumnValue = " + sourceColumnValue
													+ ", targetColumnType =" + targetColumnType + ", source type = "
													+ sourceColumnValue.getClass().getTypeName(), e);
											throw e;
										}
										// ##ORACLE_TARGET -- END TRY-catch temp
										// to catch this BLOB Copy issue.
									} else {
										// for all cases oracle/sqlserver...
										bulkWriterStatement.setNull(paramIdx, targetColumnType);
									}
								}
							}
							paramIdx += 1;
                            sourceColumnTypeIdx +=1;
						}

						bulkWriterStatement.addBatch();
					}

					final int batchCount = ctx.getDataSet().getAllResults().size();
					executeBatch(ctx.getCopyItem(), bulkWriterStatement, batchCount, ctx.getPerformanceRecorder());
					bulkWriterStatement.clearParameters();
					bulkWriterStatement.clearBatch();
					connection.commit();
					// LOG.info("$$ updating progress from data wtiter task");
					final long totalCount = ctx.getTotalCount().addAndGet(batchCount);
					updateProgress(ctx.getContext(), ctx.getCopyItem(), totalCount);
				}
			} catch (final Exception e) {
				if (connection != null) {
					connection.rollback();
				}
				throw e;
			} finally {
				if (connection != null && originalAutoCommit != null) {
					connection.setAutoCommit(originalAutoCommit);
				}
				if (connection != null && ctx != null) {
					if (requiresIdentityInsert) {
						switchIdentityInsert(connection, ctx.getCopyItem().getTargetItem(), false);
					}
					connection.close();
				}
			}
		}
	}

    private class DataBulkWriterTask extends RetriableTask {

        private DataWriterContext ctx;

        public DataBulkWriterTask(DataWriterContext ctx) {
            super(ctx.getContext(), ctx.getCopyItem().getTargetItem());
            this.ctx = ctx;
        }

        @Override
        protected Boolean internalRun() {
            try {
                if (!ctx.getDataSet().getAllResults().isEmpty()) {
                    process();
                }
                return Boolean.TRUE;
            } catch (Exception e) {
                //LOG.error("Error while executing table task " + ctx.getCopyItem().getTargetItem(),e);
                throw new RuntimeException("Error processing writer task for " + ctx.getCopyItem().getTargetItem(), e);
            }
        }

        private void process() throws Exception {
            Connection connection = null;
            Boolean originalAutoCommit = null;
            try {
                connection = ctx.getContext().getMigrationContext().getDataTargetRepository().getConnection();
                originalAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection.unwrap(SQLServerConnection.class));
                SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
                copyOptions.setBulkCopyTimeout(0);
                copyOptions.setBatchSize(ctx.getContext().getMigrationContext().getReaderBatchSize());
                bulkCopy.setBulkCopyOptions(copyOptions);
                bulkCopy.setDestinationTableName(ctx.getCopyItem().getTargetItem());

                try (Statement tempStmt = connection.createStatement();
                     ResultSet tempTargetRs = tempStmt.executeQuery(String.format("select * from %s where 0 = 1", ctx.getCopyItem().getTargetItem()))) {
                    for (String column : ctx.getColumnsToCopy()) {
                        int targetColumnIdx = tempTargetRs.findColumn(column);
                        bulkCopy.addColumnMapping(column, targetColumnIdx);
                    }
                }
                bulkCopy.writeToServer(ctx.getDataSet().toSQLServerBulkData());
                connection.commit();
                final Stopwatch timer = Stopwatch.createStarted();
                int bulkCount = ctx.getDataSet().getAllResults().size();
                LOG.debug("Bulk written ({} items) for table '{}' in {}", bulkCount, ctx.getCopyItem().getTargetItem(), timer.stop().toString());
                ctx.getPerformanceRecorder().record(PerformanceUnit.ROWS, bulkCount);
                long totalCount = ctx.getTotalCount().addAndGet(bulkCount);
                updateProgress(ctx.getContext(), ctx.getCopyItem(), totalCount);
            } catch (Exception e) {
                if (connection != null) {
                    connection.rollback();
                }
                throw e;
            } finally {
                if (connection != null && originalAutoCommit != null) {
                    connection.setAutoCommit(originalAutoCommit);
                }
                if (connection != null && ctx != null) {
                    connection.close();
                }
            }
        }
    }

    private class DataDeleteWriterTask extends RetriableTask  {

        private DataWriterContext ctx;

        public DataDeleteWriterTask(DataWriterContext ctx) {
        super(ctx.getContext(), ctx.getCopyItem().getTargetItem());
        this.ctx = ctx;
    }

        @Override
        protected Boolean internalRun() {
        try {
            if (!ctx.getDataSet().getAllResults().isEmpty()) {
                if(ctx.getContext().getMigrationContext().isDeletionEnabled()){
                    process();
                }
            }
            return Boolean.TRUE;
        } catch (Exception e) {
            //LOG.error("Error while executing table task " + ctx.getCopyItem().getTargetItem(),e);
            throw new RuntimeException("Error processing writer task for " + ctx.getCopyItem().getTargetItem(), e);
        }
    }

        private void process() throws Exception {
        Connection connection = null;
        Boolean originalAutoCommit = null;
        String PK = "PK";
        boolean requiresIdentityInsert = ctx.isRequiresIdentityInsert();
        try {
            connection = ctx.getContext().getMigrationContext().getDataTargetRepository().getConnection();
            originalAutoCommit = connection.getAutoCommit();
	        // ORACLE_TARGET - START
				String sqlDelete = "";
				if ("oracle".equalsIgnoreCase(ctx.getContext().getMigrationContext().getDataTargetRepository()
						.getDatabaseProvider().getDbName())) {
					sqlDelete = getBulkDeleteStatementOracle(ctx.getCopyItem().getTargetItem(), PK);
				} else {
					sqlDelete = getBulkDeleteStatement(ctx.getCopyItem().getTargetItem(), PK);
				}
				// ORACLE_TARGET - END
            try (PreparedStatement bulkWriterStatement = connection.prepareStatement(
                getBulkDeleteStatement(ctx.getCopyItem().getTargetItem() , PK));) {
                connection.setAutoCommit(false);
                for (List<Object> row : ctx.getDataSet().getAllResults()) {
                    int paramIdx = 1;
                    Long pkValue = (Long) ctx.getDataSet()
                        .getColumnValue("p_itempk", row);
                    bulkWriterStatement.setObject(paramIdx, pkValue);

                    paramIdx += 1;
                    bulkWriterStatement.addBatch();
                }
                int batchCount = ctx.getDataSet().getAllResults().size();
                executeBatch(ctx.getCopyItem(), bulkWriterStatement, batchCount, ctx.getPerformanceRecorder());
                bulkWriterStatement.clearParameters();
                bulkWriterStatement.clearBatch();
                connection.commit();
                long totalCount = ctx.getTotalCount().addAndGet(batchCount);
                updateProgress(ctx.getContext(), ctx.getCopyItem(), totalCount);
            }
        } catch (Exception e) {
            if (connection != null) {
                connection.rollback();
            }
            throw e;
        } finally {
            if (connection != null && originalAutoCommit != null) {
                connection.setAutoCommit(originalAutoCommit);
            }
            if (connection != null && ctx != null) {
                if (requiresIdentityInsert) {
                    switchIdentityInsert(connection, ctx.getCopyItem().getTargetItem(), false);
                }
                connection.close();
            }
        }
    }

        private List<String> getListColumn() {
        final String columns = "PK";
        if (StringUtils.isEmpty(columns)) {
            return Collections.emptyList();
        }
        List<String> result = Splitter.on(",")
            .omitEmptyStrings()
            .trimResults()
            .splitToList(columns);

        return result;
    }
    }

}
