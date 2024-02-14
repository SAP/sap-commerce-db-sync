/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceCategory;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceRecorder;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceUnit;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import de.hybris.platform.servicelayer.cluster.ClusterService;
import org.apache.commons.lang3.StringUtils;
import com.sap.cx.boosters.commercedbsync.MigrationProgress;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyBatch;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTask;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.MIGRATION_TABLESPREFIX;

/**
 * Repository to manage the status on of the migration copy tasks across the
 * cluster
 */
public class DefaultDatabaseCopyTaskRepository implements DatabaseCopyTaskRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseCopyTaskRepository.class);

    private ClusterService clusterService;

    private static final String TABLECOPYSTATUS = MIGRATION_TABLESPREFIX + "TABLECOPYSTATUS";
    private static final String TABLECOPYTASKS = MIGRATION_TABLESPREFIX + "TABLECOPYTASKS";
    private static final String TABLECOPYBATCHES = MIGRATION_TABLESPREFIX + "TABLECOPYBATCHES";

    @Override
    public String getMostRecentMigrationID(MigrationContext context) {
        String query = "SELECT migrationId FROM " + TABLECOPYSTATUS;
        try (Connection conn = getConnection(context);
                PreparedStatement stmt = conn.prepareStatement(query);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getString("migrationId");
            }
        } catch (final Exception e) {
            LOG.error("Couldn't fetch `migrationId` due to: {}", ExceptionUtils.getRootCauseMessage(e));
        }
        return null;
    }

    @Override
    public synchronized void createMigrationStatus(CopyContext context) throws Exception {
        String insert = "INSERT INTO " + TABLECOPYSTATUS + " (migrationId, total) VALUES (?, ?)";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
            stmt.setObject(1, context.getMigrationId());
            stmt.setObject(2, context.getCopyItems().size());
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void resetMigration(CopyContext context) throws Exception {
        String update = "UPDATE " + TABLECOPYSTATUS
                + " SET completed = total - failed, status = ?, failed=?, lastUpdate=? WHERE migrationId = ?";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(update)) {
            stmt.setObject(1, MigrationProgress.RUNNING.name());
            stmt.setObject(2, 0);
            stmt.setObject(3, now());
            stmt.setObject(4, context.getMigrationId());
            stmt.executeUpdate();
        }
    }

    @Override
    public void setMigrationStatus(CopyContext context, MigrationProgress progress) throws Exception {
        setMigrationStatus(context, MigrationProgress.RUNNING, progress);
    }

    @Override
    public synchronized boolean setMigrationStatus(CopyContext context, MigrationProgress from, MigrationProgress to)
            throws Exception {
        final String update = "UPDATE " + TABLECOPYSTATUS + " SET status = ? WHERE status = ? AND migrationId = ?";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(update)) {
            stmt.setObject(1, to.name());
            stmt.setObject(2, from.name());
            stmt.setObject(3, context.getMigrationId());
            return stmt.executeUpdate() > 0;
        }
    }

    @Override
    public MigrationStatus getMigrationStatus(CopyContext context) throws Exception {
        String query = "SELECT * FROM " + TABLECOPYSTATUS + " WHERE migrationId = ?";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setObject(1, context.getMigrationId());
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return convertToStatus(rs);
            }
        }
    }

    @Override
    public MigrationStatus getRunningMigrationStatus(MigrationContext context) {
        String query = "SELECT * FROM " + TABLECOPYSTATUS + " WHERE status = 'RUNNING'";
        try (Connection conn = getConnection(context);
                PreparedStatement stmt = conn.prepareStatement(query);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return convertToStatus(rs);
            }
        } catch (Exception e) {
            LOG.debug("Failed to check current migration status due to: {}", e.getMessage());
        }

        return null;
    }

    /**
     * @param rs
     *            result set to covert
     * @return the equivalent Migration Status
     * @throws Exception
     */
    public MigrationStatus convertToStatus(ResultSet rs) throws Exception {
        MigrationStatus status = new MigrationStatus();
        status.setMigrationID(rs.getString("migrationId"));
        status.setStart(getDateTime(rs, "startAt"));
        status.setEnd(getDateTime(rs, "endAt"));
        status.setLastUpdate(getDateTime(rs, "lastUpdate"));
        status.setTotalTasks(rs.getInt("total"));
        status.setCompletedTasks(rs.getInt("completed"));
        status.setFailedTasks(rs.getInt("failed"));
        status.setStatus(MigrationProgress.valueOf(rs.getString("status")));

        status.setCompleted(status.getTotalTasks() == status.getCompletedTasks()
                || MigrationProgress.STALLED == status.getStatus());
        status.setFailed(status.getFailedTasks() > 0 || MigrationProgress.STALLED == status.getStatus());
        status.setAborted(MigrationProgress.ABORTED == status.getStatus());
        status.setStatusUpdates(Collections.emptyList());

        return status;
    }

    private LocalDateTime getDateTime(ResultSet rs, String column) throws Exception {
        Timestamp ts = rs.getObject(column, Timestamp.class);
        return ts == null ? null : ts.toLocalDateTime();
    }

    @Override
    public synchronized void scheduleTask(CopyContext context, CopyContext.DataCopyItem copyItem, long sourceRowCount,
            int targetNode) throws Exception {
        String insert = "INSERT INTO " + TABLECOPYTASKS
                + " (targetnodeid, pipelinename, sourcetablename, targettablename, columnmap, migrationid, sourcerowcount, batchsize, lastupdate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
            stmt.setObject(1, targetNode);
            stmt.setObject(2, copyItem.getPipelineName());
            stmt.setObject(3, copyItem.getSourceItem());
            stmt.setObject(4, copyItem.getTargetItem());
            stmt.setObject(5, new Gson().toJson(copyItem.getColumnMap()));
            stmt.setObject(6, context.getMigrationId());
            stmt.setObject(7, sourceRowCount);
            stmt.setObject(8, copyItem.getBatchSize());
            setTimestamp(stmt, 9, now());
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void rescheduleTask(CopyContext context, String pipelineName, int targetNode) throws Exception {
        String sql = "UPDATE " + TABLECOPYTASKS
                + " SET failure='0', duration=NULL, error='',  targetnodeid=?, lastupdate=? WHERE migrationId=? AND pipelinename=? ";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, targetNode);
            setTimestamp(stmt, 2, now());
            stmt.setObject(3, context.getMigrationId());
            stmt.setObject(4, pipelineName);
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void scheduleBatch(CopyContext context, CopyContext.DataCopyItem copyItem, int batchId,
            Object lowerBoundary, Object upperBoundary) throws Exception {
        LOG.debug("Schedule Batch for {} with ID {}", copyItem.getPipelineName(), batchId);
        String insert = "INSERT INTO " + TABLECOPYBATCHES
                + " (migrationId, batchId, pipelinename, lowerBoundary, upperBoundary) VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
            stmt.setObject(1, context.getMigrationId());
            stmt.setObject(2, batchId);
            stmt.setObject(3, copyItem.getPipelineName());
            stmt.setObject(4, lowerBoundary);
            stmt.setObject(5, upperBoundary);
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void markBatchCompleted(CopyContext context, CopyContext.DataCopyItem copyItem, int batchId)
            throws Exception {
        LOG.debug("Mark batch completed for {} with ID {}", copyItem.getPipelineName(), batchId);
        String insert = "DELETE FROM " + TABLECOPYBATCHES + " WHERE migrationId=? AND batchId=? AND pipelinename=?";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
            stmt.setObject(1, context.getMigrationId());
            stmt.setObject(2, batchId);
            stmt.setObject(3, copyItem.getPipelineName());
            // exactly one batch record should be affected
            if (stmt.executeUpdate() != 1) {
                throw new IllegalStateException("No (exact) match for batch with id '" + batchId + "' found.");
            }
        }
    }

    @Override
    public synchronized void resetPipelineBatches(CopyContext context, CopyContext.DataCopyItem copyItem)
            throws Exception {
        String insert = "DELETE FROM " + TABLECOPYBATCHES + " WHERE migrationId=? AND pipelinename=?";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
            stmt.setObject(1, context.getMigrationId());
            stmt.setObject(2, copyItem.getPipelineName());
            stmt.executeUpdate();
        }
    }

    @Override
    public Set<DatabaseCopyBatch> findPendingBatchesForPipeline(CopyContext context, CopyContext.DataCopyItem item)
            throws Exception {
        String sql = "SELECT * FROM " + TABLECOPYBATCHES
                + " WHERE migrationid=? AND pipelinename=? ORDER BY batchId ASC";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, context.getMigrationId());
            stmt.setObject(2, item.getPipelineName());
            try (ResultSet resultSet = stmt.executeQuery()) {
                return convertToBatch(resultSet);
            }
        }
    }

    private Timestamp now() {
        Instant now = java.time.Instant.now();
        Timestamp ts = new Timestamp(now.toEpochMilli());
        return ts;
    }

    private Connection getConnection(CopyContext context) throws Exception {
        return getConnection(context.getMigrationContext());
    }

    private Connection getConnection(MigrationContext context) throws Exception {
        final DataRepository repository = !context.isDataExportEnabled()
                ? context.getDataTargetRepository()
                : context.getDataSourceRepository();
        /*
         * if (!repository.getDatabaseProvider().isMssqlUsed()) { throw new
         * IllegalStateException("Scheduler tables requires MSSQL database"); }
         */
        return repository.getConnection();
    }

    @Override
    public Optional<DatabaseCopyTask> findPipeline(CopyContext context, CopyContext.DataCopyItem dataCopyItem)
            throws Exception {
        String sql = "SELECT * FROM " + TABLECOPYTASKS + " WHERE targetnodeid=? AND migrationid=? AND pipelinename=?";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, getTargetNodeId());
            stmt.setObject(2, context.getMigrationId());
            stmt.setObject(3, dataCopyItem.getPipelineName());
            try (ResultSet resultSet = stmt.executeQuery()) {
                Set<DatabaseCopyTask> databaseCopyTasks = convertToTask(resultSet);
                if (databaseCopyTasks.size() > 1) {
                    throw new IllegalStateException(
                            "Invalid scheduler table, cannot have same pipeline multiple times.");
                }
                return databaseCopyTasks.stream().findFirst();
            }
        }
    }

    @Override
    public Set<DatabaseCopyTask> findPendingTasks(CopyContext context) throws Exception {
        String sql = "SELECT * FROM " + TABLECOPYTASKS
                + " WHERE targetnodeid=? AND migrationid=? AND duration IS NULL ORDER BY sourcerowcount";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, getTargetNodeId());
            stmt.setObject(2, context.getMigrationId());
            try (ResultSet resultSet = stmt.executeQuery()) {
                return convertToTask(resultSet);
            }
        }
    }

    @Override
    public Set<DatabaseCopyTask> findFailedTasks(CopyContext context) throws Exception {
        String sql = "SELECT * FROM " + TABLECOPYTASKS
                + " WHERE migrationid=? AND duration = '-1' AND failure = '1' ORDER BY sourcerowcount";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, context.getMigrationId());
            try (ResultSet resultSet = stmt.executeQuery()) {
                return convertToTask(resultSet);
            }
        }
    }

    @Override
    public synchronized void updateTaskProgress(CopyContext context, CopyContext.DataCopyItem copyItem, long itemCount)
            throws Exception {
        String sql = "UPDATE " + TABLECOPYTASKS
                + " SET targetrowcount=?, lastupdate=?, avgwriterrowthroughput=?, avgreaderrowthroughput=? WHERE targetnodeid=? AND migrationid=? AND pipelinename=?";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, itemCount);
            setTimestamp(stmt, 2, now());
            stmt.setObject(3, getAvgPerformanceValue(context, PerformanceCategory.DB_WRITE, copyItem.getTargetItem()));
            stmt.setObject(4, getAvgPerformanceValue(context, PerformanceCategory.DB_READ, copyItem.getSourceItem()));
            stmt.setObject(5, getTargetNodeId());
            stmt.setObject(6, context.getMigrationId());
            stmt.setObject(7, copyItem.getPipelineName());
            stmt.executeUpdate();
        }
    }

    protected void setTimestamp(PreparedStatement stmt, int i, Timestamp ts) throws SQLException {
        stmt.setTimestamp(i, ts, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
    }

    public void markTaskCompleted(final CopyContext context, final CopyContext.DataCopyItem copyItem,
            final String duration) throws Exception {
        markTaskCompleted(context, copyItem, duration, 0);
    }

    @Override
    public synchronized void markTaskCompleted(final CopyContext context, final CopyContext.DataCopyItem copyItem,
            final String duration, final float durationseconds) throws Exception {
        Objects.requireNonNull(duration, "duration must not be null");
        // spotless:off
        String sql = "UPDATE " + TABLECOPYTASKS + " SET duration=?, lastupdate=?, avgwriterrowthroughput=?, avgreaderrowthroughput=?, durationinseconds=? WHERE targetnodeid=? AND migrationid=? AND pipelinename=? AND duration IS NULL";
        // spotless:on
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, duration);
            setTimestamp(stmt, 2, now());
            stmt.setObject(3, getAvgPerformanceValue(context, PerformanceCategory.DB_WRITE, copyItem.getTargetItem()));
            stmt.setObject(4, getAvgPerformanceValue(context, PerformanceCategory.DB_READ, copyItem.getSourceItem()));
            stmt.setFloat(5, durationseconds);
            stmt.setObject(6, getTargetNodeId());
            stmt.setObject(7, context.getMigrationId());
            stmt.setObject(8, copyItem.getPipelineName());
            stmt.executeUpdate();
        }
        mutePerformanceRecorder(context, copyItem);
    }

    @Override
    public synchronized void markTaskFailed(CopyContext context, CopyContext.DataCopyItem copyItem, Exception error)
            throws Exception {
        // spotless:off
        String sql = "UPDATE " + TABLECOPYTASKS + " SET failure='1', duration='-1', error=?, lastupdate=? WHERE targetnodeid=? AND migrationId=? AND pipelinename=? AND failure = '0'";
        // spotless:on
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            String errorMsg = error.getMessage();
            if (StringUtils.isBlank(errorMsg)) {
                errorMsg = error.getClass().getName();
            }
            stmt.setObject(1, errorMsg.trim());
            setTimestamp(stmt, 2, now());
            stmt.setObject(3, getTargetNodeId());
            stmt.setObject(4, context.getMigrationId());
            stmt.setObject(5, copyItem.getPipelineName());
            stmt.executeUpdate();
        }
        mutePerformanceRecorder(context, copyItem);
    }

    @Override
    public synchronized void markTaskTruncated(CopyContext context, CopyContext.DataCopyItem copyItem)
            throws Exception {
        String sql = "UPDATE " + TABLECOPYTASKS
                + " SET truncated = '1' WHERE targetnodeid=? AND migrationId=? AND pipelinename=? ";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, getTargetNodeId());
            stmt.setObject(2, context.getMigrationId());
            stmt.setObject(3, copyItem.getPipelineName());
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void updateTaskCopyMethod(CopyContext context, CopyContext.DataCopyItem copyItem,
            String copyMethod) throws Exception {
        String sql = "UPDATE " + TABLECOPYTASKS
                + " SET copymethod=? WHERE targetnodeid=? AND migrationId=? AND pipelinename=? ";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, copyMethod);
            stmt.setObject(2, getTargetNodeId());
            stmt.setObject(3, context.getMigrationId());
            stmt.setObject(4, copyItem.getPipelineName());
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void updateTaskKeyColumns(CopyContext context, CopyContext.DataCopyItem copyItem,
            Collection<String> keyColumns) throws Exception {
        String sql = "UPDATE " + TABLECOPYTASKS
                + " SET keycolumns=? WHERE targetnodeid=? AND migrationId=? AND pipelinename=? ";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, Joiner.on(',').join(keyColumns));
            stmt.setObject(2, getTargetNodeId());
            stmt.setObject(3, context.getMigrationId());
            stmt.setObject(4, copyItem.getPipelineName());
            stmt.executeUpdate();
        }
    }

    @Override
    public Set<DatabaseCopyTask> getUpdatedTasks(CopyContext context, OffsetDateTime since) throws Exception {
        String sql = "SELECT * FROM " + TABLECOPYTASKS + " WHERE migrationid=? AND lastupdate >= ?";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, context.getMigrationId());
            setTimestamp(stmt, 2, toTimestamp(since));
            try (ResultSet resultSet = stmt.executeQuery()) {
                return convertToTask(resultSet);
            }
        }
    }

    private Timestamp toTimestamp(OffsetDateTime ts) {
        return new Timestamp(ts.toInstant().toEpochMilli());
    }

    @Override
    public Set<DatabaseCopyTask> getAllTasks(CopyContext context) throws Exception {
        String sql = "SELECT * FROM " + TABLECOPYTASKS + " WHERE migrationid=?";
        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, context.getMigrationId());
            try (ResultSet resultSet = stmt.executeQuery()) {
                return convertToTask(resultSet);
            }
        }
    }

    private int getTargetNodeId() {
        return clusterService.getClusterId();
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    private Set<DatabaseCopyTask> convertToTask(ResultSet rs) throws Exception {
        Set<DatabaseCopyTask> copyTasks = new HashSet<>();
        while (rs.next()) {
            DatabaseCopyTask copyTask = new DatabaseCopyTask();
            copyTask.setTargetnodeId(rs.getInt("targetnodeId"));
            copyTask.setMigrationId(rs.getString("migrationId"));
            copyTask.setPipelinename(rs.getString("pipelinename"));
            copyTask.setSourcetablename(rs.getString("sourcetablename"));
            copyTask.setTargettablename(rs.getString("targettablename"));
            copyTask.setColumnmap(new Gson().fromJson(rs.getString("columnmap"), new TypeToken<Map<String, String>>() {
            }.getType()));
            copyTask.setDuration(rs.getString("duration"));
            copyTask.setCompleted(copyTask.getDuration() != null);
            copyTask.setSourcerowcount(rs.getLong("sourcerowcount"));
            copyTask.setTargetrowcount(rs.getLong("targetrowcount"));
            copyTask.setFailure(rs.getBoolean("failure"));
            copyTask.setError(rs.getString("error"));
            copyTask.setTruncated(rs.getBoolean("truncated"));
            copyTask.setLastUpdate(getDateTime(rs, "lastupdate"));
            copyTask.setAvgReaderRowThroughput(rs.getDouble("avgreaderrowthroughput"));
            copyTask.setAvgWriterRowThroughput(rs.getDouble("avgwriterrowthroughput"));
            copyTask.setDurationinseconds(rs.getDouble("durationinseconds"));
            copyTask.setCopyMethod(rs.getString("copymethod"));
            copyTask.setKeyColumns(Splitter.on(",")
                    .splitToList(StringUtils.defaultIfEmpty(rs.getString("keycolumns"), StringUtils.EMPTY)));
            setBatchSizeSafely(copyTask, rs);
            copyTasks.add(copyTask);
        }
        return copyTasks;
    }

    // just a temporary fallback to handle ongoing migrations, where this column is
    // not yet available
    private void setBatchSizeSafely(final DatabaseCopyTask copyTask, ResultSet rs) {
        try {
            copyTask.setBatchsize(rs.getInt("batchsize"));
        } catch (SQLException e) {
            copyTask.setBatchsize(1000);
        }
    }

    private Set<DatabaseCopyBatch> convertToBatch(ResultSet rs) throws Exception {
        Set<DatabaseCopyBatch> copyBatches = new LinkedHashSet<>();
        while (rs.next()) {
            DatabaseCopyBatch copyBatch = new DatabaseCopyBatch();
            copyBatch.setMigrationId(rs.getString("migrationId"));
            copyBatch.setBatchId(rs.getString("batchId"));
            copyBatch.setPipelinename(rs.getString("pipelinename"));
            copyBatch.setLowerBoundary(rs.getString("lowerBoundary"));
            copyBatch.setUpperBoundary(rs.getString("upperBoundary"));
            copyBatches.add(copyBatch);
        }
        return copyBatches;
    }

    private double getAvgPerformanceValue(CopyContext context, PerformanceCategory category, String tableName) {
        PerformanceRecorder recorder = context.getPerformanceProfiler().getRecorder(category, tableName);
        if (recorder != null) {
            PerformanceRecorder.PerformanceAggregation performanceAggregation = recorder.getRecords()
                    .get(PerformanceUnit.ROWS);
            if (performanceAggregation != null) {
                return performanceAggregation.getAvgThroughput().get();
            }
        }
        return 0;
    }

    private void mutePerformanceRecorder(CopyContext context, CopyContext.DataCopyItem copyItem) {
        context.getPerformanceProfiler().muteRecorder(PerformanceCategory.DB_READ, copyItem.getSourceItem());
        context.getPerformanceProfiler().muteRecorder(PerformanceCategory.DB_WRITE, copyItem.getTargetItem());
    }

}
