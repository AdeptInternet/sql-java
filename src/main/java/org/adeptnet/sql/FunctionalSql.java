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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 *
 * @author Francois Steyn - Adept Internet (PTY) LTD (francois.s@adept.co.za)
 */
public class FunctionalSql {

    private static final Logger LOG = Logger.getLogger(FunctionalSql.class.getName());

    final SQLSupplier<java.sql.Connection> conProducer;

    /**
     *
     * @param conProducer SQL Supplier that will provide the SQL Connection
     */
    public FunctionalSql(final SQLSupplier<java.sql.Connection> conProducer) {
        this.conProducer = conProducer;
    }

    /**
     *
     * @return SQL Connection
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    private java.sql.Connection con() throws SQLException, SQLDataAccessException {
        return conProducer.get();
    }

    /**
     *
     * @param sql SQL Statement to be executed
     * @param params Map with Named Parameters
     * @return Stream of ResultSet
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    public Stream<ResultSet> streamFromNamedParameterQuery(final String sql, final java.util.Map<String, Object> params) throws SQLDataAccessException, SQLException {
        return stream(namedParamerterQuery(sql, params), SQLFunction.identity());
    }

    /**
     *
     * @param sql SQL Statement to be executed
     * @param params Objects for SQL Parameters
     * @return Stream of ResultSet
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    public Stream<ResultSet> streamFromParameterQuery(final String sql, final Object... params) throws SQLDataAccessException, SQLException {
        return stream(paramerterQuery(sql, params), SQLFunction.identity());
    }

    /**
     *
     * @param sql SQL Statement to be executed
     * @return Stream of ResultSet
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    public Stream<ResultSet> stream(final String sql) throws SQLException, SQLDataAccessException {
        return stream(executeQuery(sql), SQLFunction.identity());
    }

    /**
     *
     * @param resultSetSupplier Supplier for ResultSet
     * @return Stream of ResultSet
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    public Stream<ResultSet> stream(final SQLSupplier<? extends java.sql.ResultSet> resultSetSupplier) throws SQLException, SQLDataAccessException {
        return stream(resultSetSupplier, SQLFunction.identity());
    }

    /**
     *
     * @param <T> the type for the SQLFunction
     * @param resultSetSupplier SQLSupplier that will provide ResultSet
     * @param rowFunction SQLFunction that transform ResultSet to T
     * @return Stream of T
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    public <T> java.util.stream.Stream<T> stream(final SQLSupplier<? extends java.sql.ResultSet> resultSetSupplier, SQLFunction<java.sql.ResultSet, T> rowFunction) throws SQLException, SQLDataAccessException {
        final SQLResultSetIterator<T> iterator = new SQLResultSetIterator<>(resultSetSupplier.get(), rowFunction);
        return java.util.stream.StreamSupport.stream(java.util.Spliterators.spliteratorUnknownSize(iterator, 0), false)
                .onClose(() -> {
                    try {
                        iterator.close();
                    } catch (Exception ex) {
                        LOG.log(Level.SEVERE, ex.getMessage(), ex);
                    }
                });
    }

    /**
     *
     * @param <T> the type for the SQLFunction
     * @param sql SQL Statement to be executed
     * @param rowFunction SQLFunction that transforms ResultSet to T
     * @return Stream of T
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    public <T> java.util.stream.Stream<T> stream(final String sql, SQLFunction<java.sql.ResultSet, T> rowFunction) throws SQLException, SQLDataAccessException {
        return stream(executeQuery(sql), rowFunction);
    }

    /**
     *
     * @param sql SQL Statement to be executed
     * @return SQLSupplier of ResultSet
     */
    public SQLSupplier<? extends java.sql.ResultSet> executeQuery(final String sql) {
        return () -> {
            final java.util.List<java.lang.AutoCloseable> closables = new java.util.ArrayList<>(3);
            final java.sql.Connection con = con();
            closables.add(con);
            try {
                final java.sql.Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
                closables.add(stmt);
                return stmt.executeQuery(sql);
            } catch (java.sql.SQLException ex) {
                closeResources(closables);
                throw ex;
            }
        };
    }

    /**
     *
     * @param <T> the type for the SQLFunction
     * @param resultSetSupplier SQLSupplier that will provide ResultSet
     * @param resultSetFunction SQLFunction that transforms ResultSet to T
     * @return T
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    public <T> T executeQuery(SQLSupplier<? extends java.sql.ResultSet> resultSetSupplier, SQLFunction<java.sql.ResultSet, T> resultSetFunction) throws SQLException, SQLDataAccessException {
        final java.sql.ResultSet result = resultSetSupplier.get();
        try {
            return resultSetFunction.apply(result);
        } finally {
            closeResources(result);
        }
    }

    /**
     *
     * @param <T> the type for the SQLFunction
     * @param sql SQL Statement to be executed
     * @param resultSetFunction SQLFunction that transforms ResultSet to T
     * @return T
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    public <T> T executeQuery(final String sql, SQLFunction<java.sql.ResultSet, T> resultSetFunction) throws SQLException, SQLDataAccessException {
        return executeQuery(executeQuery(sql), resultSetFunction);
    }

    /**
     *
     * @param sql SQL Statement to be executed
     * @return value of java.sql.Statement.execute
     * @throws java.sql.SQLException if underlying operation throws SQLException
     */
    public boolean execute(final String sql) throws SQLException {
        return executeStatement(statement(), stmt -> stmt.execute(sql));
    }

    /**
     *
     * @param <T> the type for the SQLFunction
     * @param sqlSupplier SQLSupplier for Statement
     * @param sqlFunction SQLFunction that transforms Statement to T
     * @return T
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    public <T> T executeStatement(final SQLSupplier<? extends java.sql.Statement> sqlSupplier, SQLFunction<java.sql.Statement, T> sqlFunction) throws SQLException, SQLDataAccessException {
        final Statement stmt = sqlSupplier.get();
        try {
            return sqlFunction.apply(stmt);
        } finally {
            closeResources(stmt);
        }
    }

    /**
     *
     * @param resultSetType Same parameter as for Connection#createStatement
     * @param resultSetConcurrency Same parameter as for
     * Connection#createStatement
     * @param resultSetHoldability Same parameter as for
     * Connection#createStatement
     * @return SQLSupplier for Statement
     * @see java.sql.Connection#createStatement(int, int, int)
     */
    public SQLSupplier<? extends java.sql.Statement> statement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return () -> {
            final java.util.List<java.lang.AutoCloseable> closables = new java.util.ArrayList<>(3);
            final java.sql.Connection con = con();
            closables.add(con);
            try {
                return con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
            } catch (java.sql.SQLException ex) {
                closeResources(closables);
                throw ex;
            }
        };
    }

    /**
     *
     * @return SQLSupplier for Statement
     */
    public SQLSupplier<? extends java.sql.Statement> statement() {
        return () -> {
            final java.util.List<java.lang.AutoCloseable> closables = new java.util.ArrayList<>(3);
            final java.sql.Connection con = con();
            closables.add(con);
            try {
                return con.createStatement();
            } catch (java.sql.SQLException ex) {
                closeResources(closables);
                throw ex;
            }
        };
    }

    /**
     *
     * @param sql SQL Statement to be executed
     * @param params Map with Named Parameters
     * @return SQLSupplier for ResultSet
     */
    public SQLSupplier<? extends java.sql.ResultSet> namedParamerterQuery(final String sql, final java.util.Map<String, Object> params) {
        return () -> {
            final java.util.List<java.lang.AutoCloseable> closables = new java.util.ArrayList<>(3);
            final java.sql.Connection con = con();
            closables.add(con);
            try {
                final java.sql.Statement stmt = new NamedParameterStatement(con, sql).setAll(params).getPreparedStatement();
                closables.add(stmt);
                return stmt.executeQuery(sql);
            } catch (java.sql.SQLException ex) {
                closeResources(closables);
                throw ex;
            }
        };
    }

    /**
     *
     * @param sql SQL Statement to be executed
     * @param params Objects for SQL Parameters
     * @return SQLSupplier for ResultSet
     */
    public SQLSupplier<? extends java.sql.ResultSet> paramerterQuery(final String sql, final Object... params) {
        return () -> {
            final java.util.List<java.lang.AutoCloseable> closables = new java.util.ArrayList<>(3);
            final java.sql.Connection con = con();
            closables.add(con);
            try {
                final java.sql.PreparedStatement stmt = con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
                int cnt = 0;
                for (final Object param : params) {
                    stmt.setObject(++cnt, param);
                }
                closables.add(stmt);
                return stmt.executeQuery(sql);
            } catch (java.sql.SQLException ex) {
                closeResources(closables);
                throw ex;
            }
        };
    }

    /**
     *
     * @param closables List of AutoClosables
     */
    static void closeResources(final java.util.List<java.lang.AutoCloseable> closables) {
        for (java.lang.AutoCloseable resource : closables) {
            try {
                resource.close();
            } catch (Exception ex) {
                LOG.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
    }

    /**
     *
     * @param stmt Statement to AutoClose
     */
    static void closeResources(final java.sql.Statement stmt) {
        closeResources(getClosableResources(stmt));
    }

    /**
     *
     * @param ResultSet to AutoClose
     */
    static void closeResources(final java.sql.ResultSet rs) {
        closeResources(getClosableResources(rs));
    }

    /**
     *
     * @param ResultSet to AutoClose
     * @return List of AutoClosables
     */
    static java.util.List<java.lang.AutoCloseable> getClosableResources(final java.sql.ResultSet rs) {
        final java.util.List<java.lang.AutoCloseable> closables = new java.util.ArrayList<>(3);
        closables.add(rs);
        try {
            closables.addAll(getClosableResources(rs.getStatement()));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
        }
        return closables;
    }

    /**
     *
     * @param Statement to AutoClose
     * @return List of AutoClosables
     */
    static java.util.List<java.lang.AutoCloseable> getClosableResources(final java.sql.Statement stmt) {
        final java.util.List<java.lang.AutoCloseable> closables = new java.util.ArrayList<>(2);
        try {
            if (stmt != null) {
                closables.add(stmt);
                closables.add(stmt.getConnection());
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
        }
        return closables;
    }
}
