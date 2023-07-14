/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import com.google.common.base.Joiner;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;
import de.hybris.platform.util.Config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureIncrementalDataRepository extends AzureDataRepository {

    private static final Logger LOG = LoggerFactory.getLogger(AzureIncrementalDataRepository.class);

    private static final String PK = "PK";

    private static final String deletionTable = Config.getString("db.tableprefix", "") + "itemdeletionmarkers";

    public AzureIncrementalDataRepository(MigrationContext migrationContext,
            DataSourceConfiguration dataSourceConfiguration,
            DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(migrationContext, dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {

        if (queryDefinition.isDeletionEnabled()) {
            return buildOffsetBatchQueryForDeletion(queryDefinition, conditions);
        } else if (queryDefinition.isLpTableEnabled()) {
            return buildOffsetBatchQueryForLp(queryDefinition, conditions);
        } else {
            return super.buildOffsetBatchQuery(queryDefinition, conditions);
        }
    }

    private String buildOffsetBatchQueryForLp(OffsetQueryDefinition queryDefinition, String... conditions) {
        return String.format("SELECT * FROM %s WHERE %s ORDER BY %s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY",
                getLpTableName(queryDefinition.getTable()), expandConditions(conditions), PK,
                queryDefinition.getOffset(), queryDefinition.getBatchSize());
    }

    private String buildOffsetBatchQueryForDeletion(OffsetQueryDefinition queryDefinition, String... conditions) {
        return String.format("SELECT * FROM %s WHERE %s ORDER BY %s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY",
                deletionTable, expandConditions(conditions), queryDefinition.getOrderByColumns(),
                queryDefinition.getOffset(), queryDefinition.getBatchSize());
    }

    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        if (queryDefinition.isDeletionEnabled()) {
            return buildValueBatchQueryForDeletion(queryDefinition, conditions);
        } else {
            return super.buildValueBatchQuery(queryDefinition, conditions);
        }
    }

    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        if (queryDefinition.isDeletionEnabled()) {
            return buildBatchMarkersQueryForDeletion(queryDefinition, conditions);
        } else {
            return super.buildBatchMarkersQuery(queryDefinition, conditions);
        }
    }

    @Override
    public DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition, Instant time) throws Exception {
        if (queryDefinition.isDeletionEnabled()) {
            return getBatchOrderedByColumnForDeletion(queryDefinition, time);
        } else if (queryDefinition.isLpTableEnabled()) {
            return getBatchOrderedByColumnForLpTable(queryDefinition, time);
        } else {
            return super.getBatchOrderedByColumn(queryDefinition, time);
        }
    }

    private String buildValueBatchQueryForDeletion(SeekQueryDefinition queryDefinition, String... conditions) {
        return String.format("SELECT TOP %s * FROM %s WHERE %s ORDER BY %s", queryDefinition.getBatchSize(),
                deletionTable, expandConditions(conditions), queryDefinition.getColumn());
    }

    private DataSet getBatchOrderedByColumnForLpTable(SeekQueryDefinition queryDefinition, Instant time)
            throws Exception {
        // get batches with modifiedts >= configured time for incremental migration
        List<String> conditionsList = new ArrayList<>(2);
        processDefaultConditions(queryDefinition.getTable(), conditionsList);
        if (time != null) {
            conditionsList.add("modifiedts > ?");
        }
        if (queryDefinition.getLastColumnValue() != null) {
            conditionsList
                    .add(String.format("%s >= %s", queryDefinition.getColumn(), queryDefinition.getLastColumnValue()));
        }
        if (queryDefinition.getNextColumnValue() != null) {
            conditionsList
                    .add(String.format("%s < %s", queryDefinition.getColumn(), queryDefinition.getNextColumnValue()));
        }
        String[] conditions = null;
        List<String> pkList;
        if (conditionsList.size() > 0) {
            conditions = conditionsList.toArray(new String[conditionsList.size()]);
        }
        try (Connection connectionForPk = getConnection();
                PreparedStatement stmt = connectionForPk
                        .prepareStatement(buildValueBatchQueryForLpTable(queryDefinition, conditions))) {
            stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
            if (time != null) {
                stmt.setTimestamp(1, Timestamp.from(time));
            }
            ResultSet pkResultSet = stmt.executeQuery();
            pkList = convertToPkListForLpTable(pkResultSet);
        }

        // migrating LP Table now
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection
                        .prepareStatement(buildValueBatchQueryForLpTableWithPK(queryDefinition, pkList))) {
            // stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
            ResultSet resultSet = stmt.executeQuery();
            return convertToBatchDataSet(queryDefinition.getBatchId(), resultSet);
        }
    }

    private List<String> convertToPkListForLpTable(ResultSet resultSet) throws Exception {
        List<String> pkList = new ArrayList<>();
        while (resultSet.next()) {
            int idx = resultSet.findColumn(PK);
            pkList.add(resultSet.getString(idx));
        }
        return pkList;
    }

    private String buildValueBatchQueryForLpTableWithPK(SeekQueryDefinition queryDefinition, List<String> pkList) {
        return String.format("SELECT * FROM %s WHERE \nITEMPK IN (%s) ORDER BY %s", queryDefinition.getTable(),
                Joiner.on(", ").join(pkList), queryDefinition.getColumn());
    }

    private String buildValueBatchQueryForLpTableWithPK(OffsetQueryDefinition queryDefinition, List<String> pkList) {
        return String.format("SELECT * FROM %s WHERE \nITEMPK IN (%s)", queryDefinition.getTable(),
                Joiner.on(", ").join(pkList));
    }

    private String buildValueBatchQueryForLpTable(SeekQueryDefinition queryDefinition, String... conditions) {
        return String.format("SELECT TOP %s %s FROM %s WHERE %s ORDER BY %s", queryDefinition.getBatchSize(), PK,
                getLpTableName(queryDefinition.getTable()), expandConditions(conditions), queryDefinition.getColumn());
    }

    private String buildOffsetBatchQueryForLpTable(OffsetQueryDefinition queryDefinition, String... conditions) {
        return String.format("SELECT PK FROM %s WHERE %s ORDER BY %s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY",
                getLpTableName(queryDefinition.getTable()), expandConditions(conditions), PK,
                queryDefinition.getOffset(), queryDefinition.getBatchSize());
    }

    private DataSet getBatchOrderedByColumnForDeletion(SeekQueryDefinition queryDefinition, Instant time)
            throws Exception {
        List<String> conditionsList = new ArrayList<>(3);
        conditionsList.add("p_table = ?");
        if (time != null) {
            conditionsList.add("modifiedts > ?");
        }
        if (queryDefinition.getLastColumnValue() != null) {
            conditionsList
                    .add(String.format("%s >= %s", queryDefinition.getColumn(), queryDefinition.getLastColumnValue()));
        }
        if (queryDefinition.getNextColumnValue() != null) {
            conditionsList
                    .add(String.format("%s < %s", queryDefinition.getColumn(), queryDefinition.getNextColumnValue()));
        }
        String[] conditions = conditionsList.toArray(new String[conditionsList.size()]);
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection
                        .prepareStatement(buildValueBatchQuery(queryDefinition, conditions))) {
            stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
            stmt.setString(1, queryDefinition.getTable());

            if (time != null) {
                stmt.setTimestamp(2, Timestamp.from(time));
            }

            ResultSet resultSet = stmt.executeQuery();
            return convertToBatchDataSet(queryDefinition.getBatchId(), resultSet);
        }
    }

    @Override
    public DataSet getBatchWithoutIdentifier(OffsetQueryDefinition queryDefinition, Instant time) throws Exception {

        if (queryDefinition.isDeletionEnabled()) {
            return getBatchWithoutIdentifierForDeletion(queryDefinition, time);
        } else if (queryDefinition.isLpTableEnabled()) {
            return getBatchWithoutIdentifierForLpTable(queryDefinition, time);
        } else {
            return super.getBatchWithoutIdentifier(queryDefinition, time);
        }
    }

    private DataSet getBatchWithoutIdentifierForDeletion(OffsetQueryDefinition queryDefinition, Instant time)
            throws Exception {
        // get batches with modifiedts >= configured time for incremental migration
        List<String> conditionsList = new ArrayList<>(2);

        conditionsList.add("p_table = ?");

        if (time != null) {
            conditionsList.add("modifiedts > ?");
        }
        String[] conditions = conditionsList.toArray(new String[conditionsList.size()]);
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection
                        .prepareStatement(buildOffsetBatchQuery(queryDefinition, conditions))) {
            stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
            stmt.setString(1, queryDefinition.getTable());

            if (time != null) {
                stmt.setTimestamp(2, Timestamp.from(time));
            }
            // setting table for the deletions

            ResultSet resultSet = stmt.executeQuery();
            return convertToBatchDataSet(queryDefinition.getBatchId(), resultSet);
        }
    }

    private DataSet getBatchWithoutIdentifierForLpTable(OffsetQueryDefinition queryDefinition, Instant time)
            throws Exception {
        List<String> conditionsList = new ArrayList<>(1);
        if (time != null) {
            conditionsList.add("modifiedts > ?");
        }
        String[] conditions = null;
        if (conditionsList.size() > 0) {
            conditions = conditionsList.toArray(new String[conditionsList.size()]);
        }
        List<String> pkList;
        try (Connection connectionForPk = getConnection();
                PreparedStatement stmt = connectionForPk
                        .prepareStatement(buildOffsetBatchQueryForLpTable(queryDefinition, conditions))) {
            stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
            if (time != null) {
                stmt.setTimestamp(1, Timestamp.from(time));
            }
            ResultSet pkResultSet = stmt.executeQuery();
            pkList = convertToPkListForLpTable(pkResultSet);
        }

        // migrating LP Table now
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection
                        .prepareStatement(buildValueBatchQueryForLpTableWithPK(queryDefinition, pkList))) {
            // stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
            ResultSet resultSet = stmt.executeQuery();
            return convertToBatchDataSet(queryDefinition.getBatchId(), resultSet);
        }
    }

    @Override
    public DataSet getBatchMarkersOrderedByColumn(MarkersQueryDefinition queryDefinition, Instant time)
            throws Exception {
        if (!queryDefinition.isDeletionEnabled()) {
            return super.getBatchMarkersOrderedByColumn(queryDefinition, time);
        }

        List<String> conditionsList = new ArrayList<>(2);
        processDefaultConditions(queryDefinition.getTable(), conditionsList);

        conditionsList.add("p_table = ?");

        if (time != null) {
            conditionsList.add("modifiedts > ?");
        }

        String[] conditions = conditionsList.toArray(new String[conditionsList.size()]);

        try (Connection connection = getConnection();
                PreparedStatement stmt = connection
                        .prepareStatement(buildBatchMarkersQuery(queryDefinition, conditions))) {
            stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());

            stmt.setString(1, queryDefinition.getTable());

            if (time != null) {
                stmt.setTimestamp(2, Timestamp.from(time));
            }

            ResultSet resultSet = stmt.executeQuery();
            return convertToBatchDataSet(0, resultSet);
        }
    }

    @Override
    public long getRowCountModifiedAfter(String table, Instant time, boolean isDeletionEnabled,
            boolean lpTableMigrationEnabled) throws SQLException {
        if (isDeletionEnabled) {
            return getRowCountModifiedAfterforDeletion(table, time);
        } else if (lpTableMigrationEnabled) {
            return getRowCountModifiedAfterForLpTable(table, time);
        } else {
            return super.getRowCountModifiedAfter(table, time, false, false);
        }
    }

    private long getRowCountModifiedAfterForLpTable(String table, Instant time) throws SQLException {
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection.prepareStatement(
                        String.format("SELECT COUNT(*) FROM %s WHERE modifiedts > ?", getLpTableName(table)))) {
            stmt.setTimestamp(1, Timestamp.from(time));
            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
            return 0;
        }
    }

    private long getRowCountModifiedAfterforDeletion(String table, Instant time) throws SQLException {
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection.prepareStatement(
                        String.format("SELECT COUNT(*) FROM %s WHERE modifiedts > ? AND p_table = ?", deletionTable))) {
            stmt.setTimestamp(1, Timestamp.from(time));
            stmt.setString(2, table);
            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                return resultSet.getLong(1);
            }

            return 0;
        }
    }

    private String buildBatchMarkersQueryForDeletion(MarkersQueryDefinition queryDefinition, String... conditions) {
        String column = queryDefinition.getColumn();
        // spotless:off
        return String.format("SELECT t.%s, t.rownum\n" +
                "FROM\n" +
                "(\n" +
                "    SELECT %s, (ROW_NUMBER() OVER (ORDER BY %s))-1 AS rownum\n" +
                "    FROM %s\n WHERE %s" +
                ") AS t\n" +
                "WHERE t.rownum %% %s = 0\n" +
                "ORDER BY t.%s",
        // spotless:on
                column, column, column, deletionTable, expandConditions(conditions), queryDefinition.getBatchSize(),
                column);
    }
}
