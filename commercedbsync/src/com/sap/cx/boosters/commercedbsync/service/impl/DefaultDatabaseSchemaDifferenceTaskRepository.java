/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.sap.cx.boosters.commercedbsync.SchemaDifferenceProgress;
import com.sap.cx.boosters.commercedbsync.SchemaDifferenceStatus;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.context.SchemaDifferenceContext;
import com.sap.cx.boosters.commercedbsync.service.DatabaseSchemaDifferenceTaskRepository;
import de.hybris.platform.commercedbsynchac.data.SchemaDifferenceResultContainerData;
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

    @Override
    public String getMostRecentSchemaDifferenceId(SchemaDifferenceContext context) {
        String query = "SELECT schemaDifferenceId FROM %s ORDER BY startAt DESC".formatted(SCHEMADIFFSTATUS);
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
        String insert = "INSERT INTO %s (schemaDifferenceId) VALUES (?)".formatted(SCHEMADIFFSTATUS);
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
            stmt.setObject(1, context.getSchemaDifferenceId());
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void setSchemaDifferenceStatus(SchemaDifferenceContext context,
            SchemaDifferenceProgress progress) throws Exception {
        final String update = "UPDATE %s SET status = ? WHERE schemaDifferenceId = ?".formatted(SCHEMADIFFSTATUS);
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(update)) {
            stmt.setObject(1, progress.name());
            stmt.setObject(2, context.getSchemaDifferenceId());
            stmt.executeUpdate();
        }
    }

    @Override
    public SchemaDifferenceStatus getSchemaDifferenceStatus(SchemaDifferenceContext context) throws Exception {
        String query = "SELECT * FROM %s WHERE schemaDifferenceId = ?".formatted(SCHEMADIFFSTATUS);
        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setObject(1, context.getSchemaDifferenceId());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Unable to get schema difference status for given ID.");
                }
                return convertToStatus(rs);
            }
        }
    }

    @Override
    public void saveSqlScript(SchemaDifferenceContext context, String sqlScript) throws Exception {
        String sql = "UPDATE %s SET sqlScript = ?, lastUpdate = ? WHERE schemaDifferenceId = ?"
                .formatted(SCHEMADIFFSTATUS);

        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, sqlScript);
            stmt.setObject(2, now());
            stmt.setObject(3, context.getSchemaDifferenceId());
            stmt.executeUpdate();
        }
    }

    @Override
    public void saveDifferencesJson(SchemaDifferenceContext context, String json) throws Exception {
        String sql = "UPDATE %s SET diffJson = ?, lastUpdate = ? WHERE schemaDifferenceId = ?"
                .formatted(SCHEMADIFFSTATUS);

        try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, json);
            stmt.setObject(2, now());
            stmt.setObject(3, context.getSchemaDifferenceId());
            stmt.executeUpdate();
        }
    }

    @Override
    public synchronized void scheduleTask(SchemaDifferenceContext context, String pipelinename) throws Exception {
        String insert = "INSERT INTO %s (schemaDifferenceId, pipelineName) VALUES (?, ?)".formatted(SCHEMADIFFTASKS);
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
        String sql = "UPDATE %s SET duration = ?, durationInSeconds = ?, lastUpdate = ? WHERE schemaDifferenceId = ? AND pipelineName = ? AND duration IS NULL"
                .formatted(SCHEMADIFFTASKS);

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

        String sql = "UPDATE %s SET failure = '1', duration = '-1', error = ?, lastUpdate = ? WHERE schemaDifferenceId = ? AND pipelineName = ? AND failure = '0'"
                .formatted(SCHEMADIFFTASKS);

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
        return getConnection(context.getMigrationContext());
    }

    private Connection getConnection(MigrationContext context) throws Exception {
        return context.getDataRepository().getConnection();
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
        status.setSqlScript(StringUtils.trimToEmpty(rs.getString("sqlScript")));
        status.setDiffJson(StringUtils.trimToNull(rs.getString("diffJson")));

        return status;
    }

    private LocalDateTime getDateTime(ResultSet rs, String column) throws Exception {
        Timestamp ts = rs.getObject(column, Timestamp.class);
        return ts == null ? null : ts.toLocalDateTime();
    }
}
