/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.logging;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;

/**
 * Wrapper of {@link PreparedStatement} to add custom logging behavior when SQL
 * queries are processed by the {@link PreparedStatement}
 */
public class LoggingPreparedStatementWrapper extends LoggingStatementWrapper implements PreparedStatement {

    private final PreparedStatement preparedStatement;

    public LoggingPreparedStatementWrapper(final PreparedStatement preparedStatement, final String statementSql,
            final JDBCQueriesStore jdbcQueriesStore, final boolean logSqlParams) {
        super(preparedStatement, jdbcQueriesStore, statementSql, logSqlParams);
        this.preparedStatement = preparedStatement;
    }

    public JDBCQueriesStore getJdbcEntriesInMemoryStore() {
        return jdbcQueriesStore;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        addLogEntry();
        return preparedStatement.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        addLogEntry();
        return preparedStatement.executeUpdate();
    }

    @Override
    public boolean execute() throws SQLException {
        addLogEntry();
        return preparedStatement.execute();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        addLogEntry();
        return preparedStatement.executeLargeUpdate();
    }

    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
        preparedStatement.clearParameters();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, sqlType);
        }
        preparedStatement.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setDouble(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setBytes(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setAsciiStream(parameterIndex, x, length);
    }

    /**
     * @deprecated since 1.2
     */
    @Override
    @Deprecated(since = "1.2")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setObject(parameterIndex, x);
    }

    @Override
    public void addBatch() throws SQLException {
        // Store added batch parameters in the batch parameters list
        if (logSqlParams) {
            batchParameters.add(new HashMap<>(parameters));
        }
        // reset the current parameters map
        parameters.clear();
        preparedStatement.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, reader);
        }
        preparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return preparedStatement.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, sqlType);
        }
        preparedStatement.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return preparedStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, value);
        }
        preparedStatement.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, value);
        }
        preparedStatement.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, value);
        }
        preparedStatement.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, reader);
        }
        preparedStatement.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, inputStream);
        }
        preparedStatement.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, reader);
        }
        preparedStatement.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, xmlObject);
        }
        preparedStatement.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, reader);
        }
        preparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, reader);
        }
        preparedStatement.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, value);
        }
        preparedStatement.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, reader);
        }
        preparedStatement.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, inputStream);
        }
        preparedStatement.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, reader);
        }
        preparedStatement.setNClob(parameterIndex, reader);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        if (logSqlParams) {
            parameters.put(parameterIndex, x);
        }
        preparedStatement.setObject(parameterIndex, x, targetSqlType);
    }

}
