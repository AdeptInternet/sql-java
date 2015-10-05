/*
 * Copyright 2015 Francois Steyn - Adept Internet (PTY) LTD (francois.s@adept.co.za).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.adeptnet.sql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Francois Steyn - Adept Internet (PTY) LTD (francois.s@adept.co.za)
 */
public class NamedParameterStatement implements java.lang.AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(NamedParameterStatement.class.getName());

    private final java.sql.PreparedStatement prepStmt;
    private final java.util.Map<String, java.util.List<Integer>> fields = new java.util.HashMap<>();

    public NamedParameterStatement(final java.sql.Connection conn, final String statementWithNames) throws java.sql.SQLException {

        final Pattern findParametersPattern = Pattern.compile("(?<!')(:[\\w]+)(?!')");
        final Matcher matcher = findParametersPattern.matcher(statementWithNames);
        int pos = 1;
        final StringBuffer sb = new StringBuffer(statementWithNames.length());
        while (matcher.find()) {
            String name = matcher.group().substring(1);
            LOGGER.log(Level.INFO, "Adding {0} to name {1}", new Object[]{pos, name});
            if (!fields.containsKey(name)) {
                fields.put(name, new java.util.LinkedList<>());
            }
            fields.get(name).add(pos);
            matcher.appendReplacement(sb, "?");
            pos++;
        }
        matcher.appendTail(sb);
        LOGGER.log(Level.INFO, "Final Parameter mappings {0}", new Object[]{fields.toString()});
        prepStmt = conn.prepareStatement(sb.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public NamedParameterStatement setAll(java.util.Map<String, ?> params) throws SQLException {
        clearParameters();
        for (java.util.Map.Entry<String, ?> entry : params.entrySet()) {
            if (!fields.containsKey(entry.getKey())) {
                continue;
            }
            setObject(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public java.sql.PreparedStatement getPreparedStatement() {
        return prepStmt;
    }

    public boolean execute() throws SQLException {
        return prepStmt.execute();
    }

    public int executeUpdate() throws SQLException {
        return prepStmt.executeUpdate();
    }

    public java.sql.ResultSet executeQuery() throws java.sql.SQLException {
        return prepStmt.executeQuery();
    }

    public void addBatch() throws SQLException {
        prepStmt.addBatch();
    }

    public int getMaxFieldSize() throws SQLException {
        return prepStmt.getMaxFieldSize();
    }

    public void setMaxFieldSize(int max) throws SQLException {
        prepStmt.setMaxFieldSize(max);
    }

    public int getMaxRows() throws SQLException {
        return prepStmt.getMaxRows();
    }

    public void setMaxRows(int max) throws SQLException {
        prepStmt.setMaxRows(max);
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        prepStmt.setEscapeProcessing(enable);
    }

    public int getQueryTimeout() throws SQLException {
        return prepStmt.getQueryTimeout();
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        prepStmt.setQueryTimeout(seconds);
    }

    public void clearWarnings() throws SQLException {
        prepStmt.clearWarnings();
    }

    public void setCursorName(String name) throws SQLException {
        prepStmt.setCursorName(name);
    }

    public ResultSet getResultSet() throws SQLException {
        return prepStmt.getResultSet();
    }

    public int getUpdateCount() throws SQLException {
        return prepStmt.getUpdateCount();
    }

    public boolean getMoreResults() throws SQLException {
        return prepStmt.getMoreResults();
    }

    public void setFetchDirection(int direction) throws SQLException {
        prepStmt.setFetchDirection(direction);
    }

    public int getFetchDirection() throws SQLException {
        return prepStmt.getFetchDirection();
    }

    public void setFetchSize(int rows) throws SQLException {
        prepStmt.setFetchSize(rows);
    }

    public int getFetchSize() throws SQLException {
        return prepStmt.getFetchSize();
    }

    public int getResultSetConcurrency() throws SQLException {
        return prepStmt.getResultSetConcurrency();
    }

    public int getResultSetType() throws SQLException {
        return prepStmt.getResultSetType();
    }

    public void clearBatch() throws SQLException {
        prepStmt.clearBatch();
    }

    public int[] executeBatch() throws SQLException {
        return prepStmt.executeBatch();
    }

    public Connection getConnection() throws SQLException {
        return prepStmt.getConnection();
    }

    public boolean getMoreResults(int current) throws SQLException {
        return prepStmt.getMoreResults(current);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return prepStmt.getGeneratedKeys();
    }

    public int getResultSetHoldability() throws SQLException {
        return prepStmt.getResultSetHoldability();
    }

    public boolean isClosed() throws SQLException {
        return prepStmt.isClosed();
    }

    public void setPoolable(boolean poolable) throws SQLException {
        prepStmt.setPoolable(poolable);
    }

    public boolean isPoolable() throws SQLException {
        return prepStmt.isPoolable();
    }

    public void closeOnCompletion() throws SQLException {
        prepStmt.closeOnCompletion();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return prepStmt.isCloseOnCompletion();
    }

    public long getLargeUpdateCount() throws SQLException {
        return prepStmt.getLargeUpdateCount();
    }

    public void setLargeMaxRows(long max) throws SQLException {
        prepStmt.setLargeMaxRows(max);
    }

    public long getLargeMaxRows() throws SQLException {
        return prepStmt.getLargeMaxRows();
    }

    public long[] executeLargeBatch() throws SQLException {
        return prepStmt.executeLargeBatch();
    }

    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if ((iface.isInstance(prepStmt))) {
            return (T) prepStmt;
        } else {
            return prepStmt.unwrap(iface);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return (iface.isInstance(prepStmt)) || prepStmt.isWrapperFor(iface);
    }

    public long executeLargeUpdate() throws SQLException {
        return prepStmt.executeLargeUpdate();
    }

    @Override
    public void close() throws java.sql.SQLException {
        prepStmt.close();
    }

    public void cancel() throws SQLException {
        prepStmt.cancel();
    }

    public SQLWarning getWarnings() throws SQLException {
        return prepStmt.getWarnings();
    }

    public void setNull(final String name, int sqlType) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setNull(parameterIndex, sqlType);
        }
    }

    public void setBoolean(final String name, boolean x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setBoolean(parameterIndex, x);
        }
    }

    public void setByte(final String name, byte x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setByte(parameterIndex, x);
        }
    }

    public void setShort(final String name, short x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setShort(parameterIndex, x);
        }
    }

    public void setInt(final String name, int x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setInt(parameterIndex, x);
        }
    }

    public void setLong(final String name, long x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setLong(parameterIndex, x);
        }
    }

    public void setFloat(final String name, float x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setFloat(parameterIndex, x);
        }
    }

    public void setDouble(final String name, double x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setDouble(parameterIndex, x);
        }
    }

    public void setBigDecimal(final String name, BigDecimal x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setBigDecimal(parameterIndex, x);
        }
    }

    public void setString(final String name, String x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setString(parameterIndex, x);
        }
    }

    public void setBytes(final String name, byte[] x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setBytes(parameterIndex, x);
        }
    }

    public void setDate(final String name, Date x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setDate(parameterIndex, x);
        }
    }

    public void setTime(final String name, Time x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setTime(parameterIndex, x);
        }
    }

    public void setTimestamp(final String name, Timestamp x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setTimestamp(parameterIndex, x);
        }
    }

    public void setAsciiStream(final String name, InputStream x, int length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setAsciiStream(parameterIndex, x, length);
        }
    }

    public void setBinaryStream(final String name, InputStream x, int length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setBinaryStream(parameterIndex, x, length);
        }
    }

    public void clearParameters() throws SQLException {
        prepStmt.clearParameters();
    }

    public void setObject(final String name, Object x, int targetSqlType) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setObject(parameterIndex, x, targetSqlType);
        }
    }

    public void setObject(final String name, Object x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setObject(parameterIndex, x);
        }
    }

    public void setCharacterStream(final String name, Reader reader, int length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setCharacterStream(parameterIndex, reader, length);
        }
    }

    public void setRef(final String name, Ref x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setRef(parameterIndex, x);
        }
    }

    public void setBlob(final String name, Blob x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setBlob(parameterIndex, x);
        }
    }

    public void setClob(final String name, Clob x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setClob(parameterIndex, x);
        }
    }

    public void setArray(final String name, Array x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setArray(parameterIndex, x);
        }
    }

    public void setDate(final String name, Date x, Calendar cal) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setDate(parameterIndex, x, cal);
        }
    }

    public void setTime(final String name, Time x, Calendar cal) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setTime(parameterIndex, x, cal);
        }
    }

    public void setTimestamp(final String name, Timestamp x, Calendar cal) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setTimestamp(parameterIndex, x, cal);
        }
    }

    public void setNull(final String name, int sqlType, String typeName) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setNull(parameterIndex, sqlType, typeName);
        }
    }

    public void setURL(final String name, URL x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setURL(parameterIndex, x);
        }
    }

    public void setRowId(final String name, RowId x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setRowId(parameterIndex, x);
        }
    }

    public void setNString(final String name, String value) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setNString(parameterIndex, value);
        }
    }

    public void setNCharacterStream(final String name, Reader value, long length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setNCharacterStream(parameterIndex, value, length);
        }
    }

    public void setNClob(final String name, NClob value) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setNClob(parameterIndex, value);
        }
    }

    public void setClob(final String name, Reader reader, long length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setClob(parameterIndex, reader, length);
        }
    }

    public void setBlob(final String name, InputStream inputStream, long length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setBlob(parameterIndex, inputStream, length);
        }
    }

    public void setNClob(final String name, Reader reader, long length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setNClob(parameterIndex, reader, length);
        }
    }

    public void setSQLXML(final String name, SQLXML xmlObject) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setSQLXML(parameterIndex, xmlObject);
        }
    }

    public void setObject(final String name, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    public void setAsciiStream(final String name, InputStream x, long length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setAsciiStream(parameterIndex, x, length);
        }
    }

    public void setBinaryStream(final String name, InputStream x, long length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setBinaryStream(parameterIndex, x, length);
        }
    }

    public void setCharacterStream(final String name, Reader reader, long length) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setCharacterStream(parameterIndex, reader, length);
        }
    }

    public void setAsciiStream(final String name, InputStream x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setAsciiStream(parameterIndex, x);
        }
    }

    public void setBinaryStream(final String name, InputStream x) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setBinaryStream(parameterIndex, x);
        }
    }

    public void setCharacterStream(final String name, Reader reader) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setCharacterStream(parameterIndex, reader);
        }
    }

    public void setNCharacterStream(final String name, Reader value) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setNCharacterStream(parameterIndex, value);
        }
    }

    public void setClob(final String name, Reader reader) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setClob(parameterIndex, reader);
        }
    }

    public void setBlob(final String name, InputStream inputStream) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setBlob(parameterIndex, inputStream);
        }
    }

    public void setNClob(final String name, Reader reader) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setNClob(parameterIndex, reader);
        }
    }

    public void setObject(final String name, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    public void setObject(final String name, Object x, SQLType targetSqlType) throws SQLException {
        for (int parameterIndex : getIndices(name)) {
            prepStmt.setObject(parameterIndex, x, targetSqlType);
        }
    }

    private java.util.List<Integer> getIndices(final String paramName) throws SQLException {
        if (fields.containsKey(paramName)) {
            return fields.get(paramName);
        } else {
            throw new SQLException("Statement does not contain parameter : " + paramName);
        }
    }

    @Override
    public String toString() {
        return prepStmt.toString(); //To change body of generated methods, choose Tools | Templates.
    }

}
