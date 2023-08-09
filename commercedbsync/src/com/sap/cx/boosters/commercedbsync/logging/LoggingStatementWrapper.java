/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.logging;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * Wrapper of {@link Statement} to add custom logging behavior when SQL queries
 * are processed by the {@link Statement}
 */
public class LoggingStatementWrapper implements Statement {

    private final Statement statement;
    protected final JDBCQueriesStore jdbcQueriesStore;
    protected final String statementSql;
    protected final Map<Integer, Object> parameters;
    protected final List<Map<Integer, Object>> batchParameters;
    protected final boolean logSqlParams;

    public LoggingStatementWrapper(final Statement statement, final JDBCQueriesStore jdbcQueriesStore,
            final String statementSql, final boolean logSqlParams) {
        this.statement = statement;
        this.jdbcQueriesStore = jdbcQueriesStore;
        this.statementSql = statementSql;
        this.parameters = new HashMap<>();
        this.batchParameters = new ArrayList<>();
        this.logSqlParams = logSqlParams;
    }

    public JDBCQueriesStore getJdbcEntriesInMemoryStore() {
        return jdbcQueriesStore;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        addLogEntry(sql);
        return statement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        addLogEntry(sql);
        return statement.executeUpdate(sql);
    }

    protected void addLogEntry(String sql) {
        JdbcQueryLog logEntry = new JdbcQueryLog(sql);
        getJdbcEntriesInMemoryStore().addEntry(logEntry);
    }

    protected void addLogEntry() {
        addLogEntry(parameters);
    }

    protected void addLogEntry(Map<Integer, Object> parameters) {
        if (StringUtils.isNotBlank(statementSql)) {
            JdbcQueryLog logEntry = new JdbcQueryLog(statementSql, parameters);
            jdbcQueriesStore.addEntry(logEntry);
        }
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return statement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        statement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return statement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        statement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        statement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return statement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        statement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        statement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        statement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        statement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return statement.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return statement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return statement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return statement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        statement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return statement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        statement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return statement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return statement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return statement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        statement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        statement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (logSqlParams) {
            // When executing the batch, log a query
            // for each map of parameter values
            batchParameters.forEach(this::addLogEntry);
        } else {
            // if sql params are not logged,
            // log a single entry for the sql statement of the batch
            addLogEntry();
        }
        return statement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return statement.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return statement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return statement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return statement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return statement.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return statement.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return statement.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return statement.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return statement.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return statement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return statement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        statement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return statement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        statement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return statement.isCloseOnCompletion();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return statement.getLargeUpdateCount();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        statement.setLargeMaxRows(max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        return statement.getLargeMaxRows();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        return statement.executeLargeBatch();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return statement.executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return statement.executeLargeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return statement.executeLargeUpdate(sql, columnIndexes);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return statement.executeLargeUpdate(sql, columnNames);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return statement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return statement.isWrapperFor(iface);
    }
}
