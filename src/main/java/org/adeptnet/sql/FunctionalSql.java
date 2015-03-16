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

/**
 *
 * @author Francois Steyn - Adept Internet (PTY) LTD (francois.s@adept.co.za)
 */
public class FunctionalSql {

    final SQLSupplier<java.sql.Connection> conProducer;

    public FunctionalSql(final SQLSupplier<java.sql.Connection> conProducer) {
        this.conProducer = conProducer;
    }

    private java.sql.Connection con() throws SQLException, SQLDataAccessException {
        return conProducer.get();
    }

    public <T> java.util.stream.Stream<T> stream(final SQLSupplier<? extends java.sql.ResultSet> resultSetSupplier, SQLFunction<java.sql.ResultSet, T> rowFunction) throws SQLException, SQLDataAccessException {
        final java.sql.ResultSet result;
        result = resultSetSupplier.get();
        final SQLResultSetIterator<T> iterator = new SQLResultSetIterator<>(result, rowFunction);
        return java.util.stream.StreamSupport.stream(java.util.Spliterators.spliteratorUnknownSize(iterator, 0), false)
                .onClose(() -> {
                    try {
                        iterator.close();
                    } catch (Exception ex) {
                        Logger.getLogger(FunctionalSql.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });
    }

    public <T> T executeQuery(SQLSupplier<? extends java.sql.ResultSet> resultSetSupplier, SQLFunction<java.sql.ResultSet, T> resultSetFunction) throws SQLException, SQLDataAccessException {
        final java.sql.ResultSet result = resultSetSupplier.get();
        try {
            return resultSetFunction.apply(result);
        } finally {
            closeResources(result);
        }
    }

    public boolean execute(final String statement) throws SQLException {
        return executeStatement(statement(), stmt -> stmt.execute(statement));
    }

    public <T> T executeStatement(final SQLSupplier<? extends java.sql.Statement> statementSupplier, SQLFunction<java.sql.Statement, T> function) throws SQLException, SQLDataAccessException {
        final Statement stmt = statementSupplier.get();
        try {
            return function.apply(stmt);
        } finally {
            closeResources(stmt);
        }
    }

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

    static void closeResources(final java.util.List<java.lang.AutoCloseable> closables) {
        for (java.lang.AutoCloseable resource : closables) {
            try {
                resource.close();
            } catch (Exception ex) {
                Logger.getLogger(FunctionalSql.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    static void closeResources(final java.sql.Statement stmt) {
        closeResources(getClosableResources(stmt));
    }

    static void closeResources(final java.sql.ResultSet rs) {
        closeResources(getClosableResources(rs));
    }

    static java.util.List<java.lang.AutoCloseable> getClosableResources(final java.sql.ResultSet rs) {
        final java.util.List<java.lang.AutoCloseable> closables = new java.util.ArrayList<>(3);
        closables.add(rs);
        try {
            closables.addAll(getClosableResources(rs.getStatement()));
        } catch (Exception ex) {
            Logger.getLogger(FunctionalSql.class.getName()).log(Level.SEVERE, null, ex);
        }
        return closables;
    }

    static java.util.List<java.lang.AutoCloseable> getClosableResources(final java.sql.Statement stmt) {
        final java.util.List<java.lang.AutoCloseable> closables = new java.util.ArrayList<>(2);
        try {
            if (stmt != null) {
                closables.add(stmt);
                closables.add(stmt.getConnection());
            }
        } catch (Exception ex) {
            Logger.getLogger(FunctionalSql.class.getName()).log(Level.SEVERE, null, ex);
        }

        return closables;
    }

}
