/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.strategy.impl;

import com.google.common.base.Stopwatch;
import com.sap.cx.boosters.commercedbsync.concurrent.impl.task.RetriableTask;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceRecorder;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceUnit;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static java.sql.Types.BIGINT;

public class DataDeleteWriterTask extends RetriableTask {
    private static final Logger LOG = LoggerFactory.getLogger(DataDeleteWriterTask.class);

    private final CopyPipeWriterContext ctx;
    private final DataSet dataSet;

    public DataDeleteWriterTask(CopyPipeWriterContext ctx, DataSet dataSet) {
        super(ctx.getContext(), ctx.getCopyItem().getTargetItem());
        this.ctx = ctx;
        this.dataSet = dataSet;
    }

    @Override
    protected Boolean internalRun() {
        try {
            if (dataSet.isNotEmpty()) {
                if (ctx.getContext().getMigrationContext().isDeletionEnabled()) {
                    process();
                }
            }
            return Boolean.TRUE;
        } catch (Exception e) {
            // LOG.error("Error while executing table task " +
            // ctx.getCopyItem().getTargetItem(),e);
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

            String sqlDelete;
            String targetItem = ctx.getCopyItem().getTargetItem();
            DataBaseProvider dbProvider = ctx.getContext().getMigrationContext().getDataTargetRepository()
                    .getDatabaseProvider();

            if (dbProvider.isOracleUsed()) {
                sqlDelete = getBulkDeleteStatementOracle(targetItem, PK);
            } else if (dbProvider.isPostgreSqlUsed()) {
                sqlDelete = getBulkDeleteStatementPostgreSql(targetItem, PK);
            } else if (dbProvider.isMySqlUsed()) {
                sqlDelete = getBulkDeleteStatementMySql(targetItem, PK);
            } else {
                sqlDelete = getBulkDeleteStatement(targetItem, PK);
            }

            try (PreparedStatement bulkWriterStatement = connection.prepareStatement(sqlDelete)) {
                connection.setAutoCommit(false);
                for (List<Object> row : dataSet.getAllResults()) {
                    Long pkValue = (Long) dataSet.getColumnValue("p_itempk", row, dataSet.getColumn(1), BIGINT);
                    bulkWriterStatement.setObject(1, pkValue);
                    bulkWriterStatement.addBatch();
                }
                int batchCount = dataSet.getAllResults().size();
                executeBatch(ctx.getCopyItem(), bulkWriterStatement, batchCount, ctx.getPerformanceRecorder());
                bulkWriterStatement.clearParameters();
                bulkWriterStatement.clearBatch();
                connection.commit();
                long totalCount = ctx.getTotalCount().addAndGet(batchCount);
                ctx.getDatabaseCopyTaskRepository().updateTaskProgress(ctx.getContext(), ctx.getCopyItem(), totalCount);
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

    protected void executeBatch(CopyContext.DataCopyItem item, PreparedStatement preparedStatement, long batchCount,
            PerformanceRecorder recorder) throws SQLException {
        final Stopwatch timer = Stopwatch.createStarted();
        preparedStatement.executeBatch();
        preparedStatement.clearBatch();
        LOG.debug("Batch written ({} items) for table '{}' in {}", batchCount, item.getTargetItem(), timer.stop());
        recorder.record(PerformanceUnit.ROWS, batchCount);
    }

    private void switchIdentityInsert(Connection connection, final String tableName, boolean on) {
        try (Statement stmt = connection.createStatement()) {
            String onOff = on ? "ON" : "OFF";
            stmt.executeUpdate(String.format("SET IDENTITY_INSERT %s %s", tableName, onOff));
        } catch (final Exception e) {
            throw new RuntimeException("Could not switch identity insert", e);
        }
    }

    private String getBulkDeleteStatement(String targetTableName, String columnId) {
        /*
         * https://michaeljswart.com/2017/07/sql-server-upsert-patterns-and-
         * antipatterns/ We are not using a stored procedure here as CCv2 does not grant
         * sp exec permission to the default db user
         */
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("MERGE %s WITH (HOLDLOCK) AS t", targetTableName));
        sqlBuilder.append("\n");
        sqlBuilder.append(String.format("USING (SELECT %s) AS s ON t.%s = s.%s", "? " + columnId, columnId, columnId));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN MATCHED THEN DELETE"); // DELETE
        sqlBuilder.append(";");
        LOG.debug("MERGE-DELETE SQL Server " + sqlBuilder);
        return sqlBuilder.toString();
    }

    private String getBulkDeleteStatementOracle(final String targetTableName, final String columnId) {
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("MERGE INTO  %s  t", targetTableName));
        sqlBuilder.append("\n");
        sqlBuilder.append(
                String.format("USING (SELECT %s from dual) s ON (t.%s = s.%s)", "? " + columnId, columnId, columnId));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN MATCHED THEN "); // DELETE
        sqlBuilder.append("UPDATE SET t.HJMPTS = 0 "); // IS INSERT OR UPDATE
        sqlBuilder.append("DELETE WHERE " + String.format(" t.%s = s.%s ", columnId, columnId));// DELETE
        LOG.debug("MERGE-DELETE ORACLE " + sqlBuilder);
        return sqlBuilder.toString();
    }

    private String getBulkDeleteStatementMySql(final String targetTableName, final String columnId) {
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("DELETE t FROM %s t", targetTableName));
        sqlBuilder.append("\n");
        sqlBuilder.append(String.format("JOIN (SELECT ? AS %s) s ON t.%s = s.%s", columnId, columnId, columnId));
        LOG.debug("DELETE MYSQL " + sqlBuilder);
        return sqlBuilder.toString();
    }

    // PostgreSql >= 15
    private String getBulkDeleteStatementPostgreSql(final String targetTableName, final String columnId) {
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("MERGE INTO %s AS t", targetTableName));
        sqlBuilder.append("\n");
        sqlBuilder.append(String.format("USING (SELECT ? AS %s) AS s ON (t.%s = s.%s)", columnId, columnId, columnId));
        sqlBuilder.append("\n");
        sqlBuilder.append("WHEN MATCHED THEN DELETE");
        sqlBuilder.append(";");
        LOG.debug("MERGE-DELETE PostgreSQL: " + sqlBuilder);
        return sqlBuilder.toString();
    }
}
