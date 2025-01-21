/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.strategy.impl;

import com.google.common.base.Stopwatch;
import com.sap.cx.boosters.commercedbsync.anonymizer.TextEvaluator;
import com.sap.cx.boosters.commercedbsync.anonymizer.TextTokenizer;
import com.sap.cx.boosters.commercedbsync.anonymizer.model.AnonymizerConfiguration;
import com.sap.cx.boosters.commercedbsync.anonymizer.model.Column;
import com.sap.cx.boosters.commercedbsync.anonymizer.model.Table;
import com.sap.cx.boosters.commercedbsync.concurrent.impl.task.RetriableTask;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataColumn;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceRecorder;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceUnit;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.sql.Types.*;

class CopyPipeWriterTask extends RetriableTask {
    private static final Logger LOG = LoggerFactory.getLogger(CopyPipeWriterTask.class);

    private final CopyPipeWriterContext ctx;
    private final DataSet dataSet;
    private final AnonymizerConfiguration anonymizerConfiguration;

    public CopyPipeWriterTask(final CopyPipeWriterContext ctx, final DataSet dataSet,
            final AnonymizerConfiguration anonymizerConfiguration) {
        super(ctx.getContext(), ctx.getCopyItem().getTargetItem());
        this.ctx = ctx;
        this.dataSet = dataSet;
        this.anonymizerConfiguration = anonymizerConfiguration;
    }

    @Override
    protected Boolean internalRun() {
        try {
            if (dataSet.isNotEmpty()) {
                process();
            }
            return Boolean.TRUE;
        } catch (Exception e) {
            throw new RuntimeException("Error processing writer task for " + ctx.getCopyItem().getTargetItem(), e);
        }
    }

    private boolean isColumnOverride(CopyContext.DataCopyItem item, String sourceColumnName) {
        return MapUtils.isNotEmpty(item.getColumnMap()) && item.getColumnMap().containsKey(sourceColumnName);
    }

    private void switchIdentityInsert(Connection connection, final String tableName, boolean on) {
        try (Statement stmt = connection.createStatement()) {
            String onOff = on ? "ON" : "OFF";
            stmt.executeUpdate(String.format("SET IDENTITY_INSERT %s %s", tableName, onOff));
        } catch (final Exception e) {
            // TODO using brute force FIX
            // throw new RuntimeException("Could not switch identity insert", e);
        }
    }

    private String getBulkInsertStatement(String targetTableName, List<String> columnsToCopy,
            List<String> columnsToCopyValues) {
        return "INSERT INTO " + targetTableName + " "
                + getBulkInsertStatementParamList(columnsToCopy, columnsToCopyValues);
    }

    private String getBulkInsertStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues) {
        return "(" + String.join(", ", columnsToCopy) + ") VALUES (" + String.join(", ", columnsToCopyValues) + ")";
    }

    private PreparedStatement createPreparedStatement(final CopyContext context, final String targetTableName,
            final List<String> columnsToCopy, final List<String> upsertIds, final Connection targetConnection)
            throws Exception {
        if (context.getMigrationContext().isIncrementalModeEnabled()) {
            if (!upsertIds.isEmpty()) {
                final String upsertStatement = context.getMigrationContext().getDataTargetRepository()
                        .buildBulkUpsertStatement(targetTableName, columnsToCopy, upsertIds);

                LOG.debug("Upsert statement for: {}\n{}",
                        context.getMigrationContext().getDataTargetRepository().getDatabaseProvider(), upsertStatement);

                return targetConnection.prepareStatement(upsertStatement);
            } else {
                throw new RuntimeException(
                        "The incremental approach can only be used on tables that have a valid identifier like PK or ID");
            }
        } else {
            return targetConnection.prepareStatement(getBulkInsertStatement(targetTableName, columnsToCopy,
                    columnsToCopy.stream().map(column -> "?").collect(Collectors.toList())));
        }
    }

    private void executeBatch(CopyContext.DataCopyItem item, PreparedStatement preparedStatement, long batchCount,
            PerformanceRecorder recorder) throws SQLException {
        final Stopwatch timer = Stopwatch.createStarted();
        preparedStatement.executeBatch();
        preparedStatement.clearBatch();
        LOG.debug("Batch written ({} items) for table '{}' in {}", batchCount, item.getTargetItem(), timer.stop());
        recorder.record(PerformanceUnit.ROWS, batchCount);
    }

    private Object anonymize(final String tableName, final String columnName, final Object columnValue) {
        if (!anonymizerConfiguration.getTables().contains(new Table(tableName))) {
            return columnValue;
        }
        final Table table = anonymizerConfiguration.getTable(tableName);
        if (table == null) {
            return columnValue;
        }
        final Column column = table.getColumn(columnName);
        if (column == null) {
            return columnValue;
        }
        if (column.getExclude().contains(columnValue)) {
            return columnValue;
        }

        final TextTokenizer textTokenizer = new TextTokenizer();
        final TextEvaluator textEvaluator = new TextEvaluator();
        final List<String> tokens = textTokenizer.tokenizeText(column.getText());

        switch (column.getOperation()) {
            case APPEND :
                return columnValue == null ? "" : columnValue + textEvaluator.getForTokens(tokens);
            case REPLACE :
                return textEvaluator.getForTokens(tokens);
        }

        return null;
    }

    private Map<Column, Object> getColumnValuesAnonymized(final DataBaseProvider dbProvider,
            final ResultSet tempTargetRs, final List<Object> row, final String tableName) throws SQLException {
        final Table table = anonymizerConfiguration.getTable(tableName);
        if (table == null) {
            return Collections.emptyMap();
        }
        final Map<Column, Object> columnValuesAnonymized = new HashMap<>();
        int paramIdx = 0;
        for (final String sourceColumnName : ctx.getColumnsToCopy()) {
            final Column column = table.getColumn(sourceColumnName);
            if (column == null) {
                continue;
            }
            final int targetColumnIdx = tempTargetRs.findColumn(sourceColumnName);
            final DataColumn sourceColumnType = dataSet.getColumn(paramIdx++);
            final int targetColumnType = tempTargetRs.getMetaData().getColumnType(targetColumnIdx);
            if (!ctx.getNullifyColumns().contains(sourceColumnName)
                    && !isColumnOverride(ctx.getCopyItem(), sourceColumnName)) {
                Object sourceColumnValue = dataSet.getColumnValue(sourceColumnName, row, sourceColumnType,
                        targetColumnType);
                if (sourceColumnValue != null) {
                    if (column.getExcludeRow().contains(sourceColumnValue)) {
                        return Collections.emptyMap();
                    }
                    columnValuesAnonymized.put(column, anonymize(tableName, sourceColumnName, sourceColumnValue));
                }
            }
        }
        return columnValuesAnonymized;
    }

    private Object getColumnValue(final String tableName, final String columnName, Map<Column, Object> columnValues) {
        final Table table = anonymizerConfiguration.getTable(tableName);
        if (table == null) {
            return null;
        }
        final Column column = table.getColumn(columnName);
        if (column == null) {
            return null;
        }
        return columnValues.get(column);
    }

    private void process() throws Exception {
        Connection connection = null;
        Boolean originalAutoCommit = null;
        boolean requiresIdentityInsert = ctx.isRequiresIdentityInsert();
        try {
            connection = ctx.getContext().getMigrationContext().getDataTargetRepository().getConnection();

            final DataBaseProvider dbProvider = ctx.getContext().getMigrationContext().getDataTargetRepository()
                    .getDatabaseProvider();
            final String tableName = ctx.getCopyItem().getTargetItem();
            LOG.debug("TARGET DB name = " + dbProvider.getDbName() + " SOURCE TABLE = "
                    + ctx.getCopyItem().getSourceItem() + ", TARGET Table = " + tableName);

            originalAutoCommit = connection.getAutoCommit();

            try (PreparedStatement bulkWriterStatement = createPreparedStatement(ctx.getContext(),
                    ctx.getCopyItem().getTargetItem(), ctx.getColumnsToCopy(), ctx.getUpsertIds(), connection);
                    Statement tempStmt = connection.createStatement();
                    ResultSet tempTargetRs = tempStmt.executeQuery(
                            String.format("select * from %s where 0 = 1", ctx.getCopyItem().getTargetItem()))) {
                connection.setAutoCommit(false);
                if (requiresIdentityInsert) {
                    switchIdentityInsert(connection, ctx.getCopyItem().getTargetItem(), true);
                }

                boolean printedBlobLog = false;
                boolean printedClobLog = false;

                for (List<Object> row : dataSet.getAllResults()) {
                    final Map<Column, Object> columnValuesAnonymized = ctx.getContext().getMigrationContext()
                            .isAnonymizerEnabled()
                                    ? getColumnValuesAnonymized(dbProvider, tempTargetRs, row, tableName)
                                    : null;
                    int paramIdx = 1;
                    for (String sourceColumnName : ctx.getColumnsToCopy()) {
                        int targetColumnIdx = tempTargetRs.findColumn(sourceColumnName);
                        DataColumn sourceColumnType = dataSet.getColumn(paramIdx - 1);
                        int targetColumnType = tempTargetRs.getMetaData().getColumnType(targetColumnIdx);
                        if (ctx.getNullifyColumns().contains(sourceColumnName)) {
                            bulkWriterStatement.setNull(paramIdx, targetColumnType);
                            LOG.trace("Column {} is nullified. Setting NULL value...", sourceColumnName);
                        } else {
                            if (isColumnOverride(ctx.getCopyItem(), sourceColumnName)) {
                                bulkWriterStatement.setObject(paramIdx,
                                        ctx.getCopyItem().getColumnMap().get(sourceColumnName), targetColumnType);
                            } else {
                                final Object anonymizedValue = columnValuesAnonymized == null
                                        ? null
                                        : columnValuesAnonymized.get(new Column(sourceColumnName));
                                Object sourceColumnValue = anonymizedValue != null
                                        ? anonymizedValue
                                        : dataSet.getColumnValue(sourceColumnName, row, sourceColumnType,
                                                targetColumnType);
                                if (sourceColumnValue != null) {
                                    /*
                                     * Code to handle \u0000 (NULL) characters in PostgreSQL as those are not
                                     * allowed within text fields, exception example: PSQLException: ERROR: invalid
                                     * byte sequence for encoding "UTF8": 0x00
                                     */
                                    if (dbProvider.isPostgreSqlUsed() && sourceColumnValue instanceof String
                                            && targetColumnType == VARCHAR) {
                                        final String stringValue = (String) sourceColumnValue;
                                        sourceColumnValue = StringUtils.remove(stringValue, Character.MIN_VALUE);
                                    }

                                    // catch all exceptions, not to print each time, print one for each type/worker.
                                    try {
                                        if (!dbProvider.isOracleUsed()) {
                                            // for all cases non-oracle
                                            bulkWriterStatement.setObject(paramIdx, sourceColumnValue,
                                                    targetColumnType);
                                        } else {
                                            switch (targetColumnType) {
                                                /*
                                                 * code to handle BLOB, because setObject throws exception example
                                                 * Products.p_buyerids is varbinary(max) in (sqlserver) AND blob in
                                                 * (oracle)
                                                 */
                                                case BLOB : {
                                                    if (!printedBlobLog) {
                                                        LOG.debug("BLOB 2004 sourceColumnName = " + sourceColumnName
                                                                + " souce value type CN = "
                                                                + sourceColumnValue.getClass().getCanonicalName()
                                                                + " , Name = " + sourceColumnValue.getClass().getName()
                                                                + " , Type Name = "
                                                                + sourceColumnValue.getClass().getTypeName());
                                                        printedBlobLog = true;
                                                    }
                                                    bulkWriterStatement.setBytes(paramIdx, (byte[]) sourceColumnValue);
                                                    break;

                                                }
                                                /*
                                                 * code to handle CLOB, because setObject throws exception example
                                                 * Promotion.description is nvarchar(max) in (sqlserver) AND blob in
                                                 * (oracle)
                                                 */
                                                case CLOB : {
                                                    if (!printedClobLog) {
                                                        LOG.debug("CLOB 2005 sourceColumnName = " + sourceColumnName
                                                                + " souce value type CN = "
                                                                + sourceColumnValue.getClass().getCanonicalName()
                                                                + " , Name = " + sourceColumnValue.getClass().getName()
                                                                + " , Type Name = "
                                                                + sourceColumnValue.getClass().getTypeName());
                                                        printedClobLog = true;
                                                    }
                                                    if (sourceColumnValue instanceof String) {
                                                        final String clobString = (String) sourceColumnValue;
                                                        if (!clobString.isEmpty()) {
                                                            LOG.debug(" reading CLOB");
                                                            bulkWriterStatement.setClob(paramIdx,
                                                                    new StringReader(clobString), clobString.length());
                                                            LOG.debug(" wrote CLOB");
                                                        } else {
                                                            LOG.debug("CLOB is empty... setting null");
                                                            bulkWriterStatement.setNull(paramIdx, targetColumnType);
                                                        }
                                                    }
                                                    break;
                                                }
                                                default : {
                                                    bulkWriterStatement.setObject(paramIdx, sourceColumnValue,
                                                            targetColumnType);
                                                    break;
                                                }
                                            }
                                        }
                                    } catch (final NumberFormatException e) {
                                        LOG.error("NumberFormatException - Error setting Type on sourceColumnName = "
                                                + sourceColumnName + ", sourceColumnValue = " + sourceColumnValue
                                                + ", targetColumnType =" + targetColumnType + ", source type = "
                                                + sourceColumnValue.getClass().getTypeName());
                                        if (dbProvider.isOracleUsed() && targetColumnType == NUMERIC
                                                && sourceColumnValue instanceof String) {
                                            final String stringValue = (String) sourceColumnValue;
                                            if (!stringValue.isEmpty()) {
                                                final int character = Character.codePointAt((String) sourceColumnValue,
                                                        0);
                                                bulkWriterStatement.setInt(paramIdx, character);
                                            }
                                        }
                                    } catch (final Exception e) {
                                        LOG.error("Error setting Type on sourceColumnName = " + sourceColumnName
                                                + ", sourceColumnValue = " + sourceColumnValue + ", targetColumnType ="
                                                + targetColumnType + ", source type = "
                                                + sourceColumnValue.getClass().getTypeName(), e);
                                        throw e;
                                    }
                                } else {
                                    bulkWriterStatement.setNull(paramIdx, targetColumnType);
                                }
                            }
                        }
                        paramIdx++;
                    }
                    bulkWriterStatement.addBatch();
                }
                int batchCount = dataSet.getAllResults().size();
                executeBatch(ctx.getCopyItem(), bulkWriterStatement, batchCount, ctx.getPerformanceRecorder());
                bulkWriterStatement.clearParameters();
                bulkWriterStatement.clearBatch();
                connection.commit();
                ctx.getDatabaseCopyTaskRepository().markBatchCompleted(ctx.getContext(), ctx.getCopyItem(),
                        dataSet.getBatchId(), dataSet.getPartition());
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
            if (connection != null) {
                if (requiresIdentityInsert) {
                    switchIdentityInsert(connection, ctx.getCopyItem().getTargetItem(), false);
                }
                connection.close();
            }
        }
    }
}
