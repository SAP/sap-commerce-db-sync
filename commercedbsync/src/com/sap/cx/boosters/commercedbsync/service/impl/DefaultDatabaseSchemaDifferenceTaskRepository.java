/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.sap.cx.boosters.commercedbsync.SchemaDifferenceProgress;
import com.sap.cx.boosters.commercedbsync.SchemaDifferenceStatus;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.context.SchemaDifferenceContext;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseSchemaDifferenceTaskRepository;
import de.hybris.platform.commercedbsynchac.data.SchemaDifferenceResultContainerData;
import de.hybris.platform.commercedbsynchac.data.SchemaDifferenceResultData;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.MIGRATION_TABLESPREFIX;

public class DefaultDatabaseSchemaDifferenceTaskRepository implements DatabaseSchemaDifferenceTaskRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseSchemaDifferenceTaskRepository.class);

    private static final String SCHEMADIFFSTATUS = MIGRATION_TABLESPREFIX + "SCHEMADIFFSTATUS";
    private static final String SCHEMADIFFTASKS = MIGRATION_TABLESPREFIX + "SCHEMADIFFTASKS";
    private static final String SCHEMADIFFS = MIGRATION_TABLESPREFIX + "SCHEMADIFFS";

    @Override
    public String getMostRecentSchemaDifferenceId(SchemaDifferenceContext context) {
        String query = "SELECT schemaDifferenceId FROM " + SCHEMADIFFSTATUS + " ORDER BY startAt DESC";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(query)) {
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("schemaDifferenceId");
            }
        } catch (final Exception e) {
            LOG.error("Couldn't fetch `schemaDifferenceId` due to: {}", ExceptionUtils.getRootCauseMessage(e));
        }
        return null;
    }

    @Override
    public synchronized void createSchemaDifferenceStatus(SchemaDifferenceContext context) throws Exception {
        String insert = "INSERT INTO " + SCHEMADIFFSTATUS + " (schemaDifferenceId) VALUES (?)";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
            stmt.setObject(1, context.getSchemaDifferenceId());
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void setSchemaDifferenceStatus(SchemaDifferenceContext context,
            SchemaDifferenceProgress progress) throws Exception {
        final String update = "UPDATE " + SCHEMADIFFSTATUS + " SET status = ? WHERE schemaDifferenceId = ?";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(update)) {
            stmt.setObject(1, progress.name());
            stmt.setObject(2, context.getSchemaDifferenceId());
            stmt.executeUpdate();
        }
    }

    @Override
    public SchemaDifferenceStatus getSchemaDifferenceStatus(SchemaDifferenceContext context) throws Exception {
        String query = "SELECT * FROM " + SCHEMADIFFSTATUS + " WHERE schemaDifferenceId = ?";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setObject(1, context.getSchemaDifferenceId());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Unable to get schema difference status for given ID.");
                }
                SchemaDifferenceStatus status = convertToStatus(rs);
                status.getDiffResult().setSource(getSchemaDifferenceResultData(context, "source"));
                status.getDiffResult().setTarget(getSchemaDifferenceResultData(context, "target"));
                return status;
            }
        }
    }

    @Override
    public SchemaDifferenceResultData getSchemaDifferenceResultData(SchemaDifferenceContext context,
            String referenceDatabase) throws Exception {
        String query = "SELECT missingTableLeftName, missingTableRightName, missingColumnName FROM " + SCHEMADIFFS
                + " WHERE schemaDifferenceId = ? AND referenceDatabase = ?";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setObject(1, context.getSchemaDifferenceId());
            stmt.setObject(2, referenceDatabase);
            try (ResultSet rs = stmt.executeQuery()) {
                return convertToResult(rs);
            }
        }
    }

    @Override
    public void saveSchemaDifference(SchemaDifferenceContext context,
            DefaultDatabaseSchemaDifferenceService.SchemaDifference schemaDifference, String referenceDatabase)
            throws Exception {
        String insert = "INSERT INTO " + SCHEMADIFFS
                + " (schemaDifferenceId, referenceDatabase, missingTableLeftName, missingTableRightName, missingColumnName) VALUES (?, ?, ?, ?, ?)";

        List<String[]> data = new ArrayList<>();
        schemaDifference.getMissingTables().forEach(missingTable -> {
            data.add(new String[]{missingTable.getLeftName(), missingTable.getRightName(), StringUtils.EMPTY});
        });

        schemaDifference.getMissingColumnsInTable().asMap().forEach((tableKeyPair, columns) -> {
            columns.forEach(column -> {
                data.add(new String[]{tableKeyPair.getLeftName(), tableKeyPair.getRightName(), column});
            });
        });

        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(insert)) {
            for (String[] result : data) {
                stmt.setObject(1, context.getSchemaDifferenceId());
                stmt.setObject(2, referenceDatabase);
                stmt.setObject(3, result[0]);
                stmt.setObject(4, result[1]);
                stmt.setObject(5, result[2]);
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    @Override
    public void saveSqlScript(SchemaDifferenceContext context, String sqlScript) throws Exception {
        String sql = "UPDATE " + SCHEMADIFFSTATUS + " SET sqlScript = ?, lastupdate = ? WHERE schemaDifferenceId = ?";

        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, sqlScript);
            stmt.setObject(2, now());
            stmt.setObject(3, context.getSchemaDifferenceId());
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void scheduleTask(SchemaDifferenceContext context, String pipelinename) throws Exception {
        String insert = "INSERT INTO " + SCHEMADIFFTASKS + " (schemaDifferenceId, pipelinename) VALUES (?, ?)";
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
            stmt.setObject(1, context.getSchemaDifferenceId());
            stmt.setObject(2, pipelinename);
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void markTaskCompleted(final SchemaDifferenceContext context, final String pipelinename,
            final String duration, final float durationseconds) throws Exception {
        Objects.requireNonNull(duration, "duration must not be null");
        String sql = "UPDATE " + SCHEMADIFFTASKS
                + " SET duration = ?, durationinseconds = ?, lastupdate = ? WHERE schemaDifferenceId = ? AND pipelinename = ? AND duration IS NULL";

        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, duration);
            stmt.setObject(2, durationseconds);
            stmt.setObject(3, now());
            stmt.setObject(4, context.getSchemaDifferenceId());
            stmt.setObject(5, pipelinename);
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void markTaskFailed(SchemaDifferenceContext context, String pipelinename, Exception error)
            throws Exception {

        String sql = "UPDATE " + SCHEMADIFFTASKS
                + " SET failure = '1', duration = '-1', error = ?, lastupdate = ? WHERE schemaDifferenceId = ? AND pipelinename = ? AND failure = '0'";

        try (Connection connection = getConnection(context);
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            String errorMsg = error.getMessage();
            if (StringUtils.isBlank(errorMsg)) {
                errorMsg = error.getClass().getName();
            }
            stmt.setObject(1, errorMsg.trim());
            stmt.setObject(2, now());
            stmt.setObject(3, context.getSchemaDifferenceId());
            stmt.setObject(4, pipelinename);
            stmt.executeUpdate();
        }
    }

    private Timestamp now() {
        Instant now = java.time.Instant.now();
        return new Timestamp(now.toEpochMilli());
    }

    private Connection getConnection(SchemaDifferenceContext context) throws Exception {
        final MigrationContext migrationContext = context.getMigrationContext();

        final DataRepository repository = !migrationContext.isDataSynchronizationEnabled()
                ? migrationContext.getDataTargetRepository()
                : migrationContext.getDataSourceRepository();

        return repository.getConnection();
    }

    /**
     * @param rs
     *            result set to convert
     * @return the equivalent SchemaDifferenceStatus
     * @throws Exception
     */
    private SchemaDifferenceStatus convertToStatus(ResultSet rs) throws Exception {
        SchemaDifferenceStatus status = new SchemaDifferenceStatus();
        status.setSchemaDifferenceId(rs.getString("schemaDifferenceId"));
        status.setStart(getDateTime(rs, "startAt"));
        status.setEnd(getDateTime(rs, "endAt"));
        status.setLastUpdate(getDateTime(rs, "lastUpdate"));
        status.setStatus(SchemaDifferenceProgress.valueOf(rs.getString("status")));
        status.setCompleted(rs.getInt("total") == rs.getInt("completed"));
        status.setFailed(rs.getInt("failed") > 0);
        status.setAborted(SchemaDifferenceProgress.ABORTED == status.getStatus());
        status.setDiffResult(new SchemaDifferenceResultContainerData());
        final String sqlScript = rs.getString("sqlScript");
        status.setSqlScript(sqlScript == null ? StringUtils.EMPTY : sqlScript);

        return status;
    }

    /**
     * @param rs
     *            result set to convert
     * @return the equivalent SchemaDifferenceResultData
     * @throws Exception
     */
    private SchemaDifferenceResultData convertToResult(ResultSet rs) throws Exception {
        SchemaDifferenceResultData resultData = new SchemaDifferenceResultData();

        Map<DefaultDatabaseSchemaDifferenceService.TableKeyPair, String> map = new HashMap<>();
        while (rs.next()) {
            final DefaultDatabaseSchemaDifferenceService.TableKeyPair tableKeyPair = new DefaultDatabaseSchemaDifferenceService.TableKeyPair(
                    rs.getString("missingTableLeftName"), rs.getString("missingTableRightName"));
            final String missingColumnName = rs.getString("missingColumnName") == null
                    ? StringUtils.EMPTY
                    : rs.getString("missingColumnName");
            map.merge(tableKeyPair, missingColumnName,
                    (oldValue, newValue) -> String.join(MISSING_COLUMN_DELIMITER, oldValue, newValue));
        }

        String[][] results = new String[map.size()][3];
        int count = 0;
        for (Map.Entry<DefaultDatabaseSchemaDifferenceService.TableKeyPair, String> entry : map.entrySet()) {
            results[count][0] = entry.getKey().getLeftName();
            results[count][1] = entry.getKey().getRightName();
            results[count][2] = entry.getValue();
            count++;
        }
        resultData.setResults(results);

        return resultData;
    }

    private LocalDateTime getDateTime(ResultSet rs, String column) throws Exception {
        Timestamp ts = rs.getObject(column, Timestamp.class);
        return ts == null ? null : ts.toLocalDateTime();
    }
}
