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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Francois Steyn - Adept Internet (PTY) LTD (francois.s@adept.co.za)
 * @param <T> Result Type of applying row function on ResultSet
 */
public class SQLResultSetIterator<T> implements java.util.Iterator<T> {

    private static final Logger LOG = Logger.getLogger(SQLResultSetIterator.class.getName());

    private final SQLFunction<java.sql.ResultSet, T> rowFunction;
    private final java.sql.ResultSet rs;
    private final java.sql.ResultSet wrapper;
    private Boolean hasNext;

    public SQLResultSetIterator(final java.sql.ResultSet rs, final SQLFunction<java.sql.ResultSet, T> rowFunction) {
        this.rowFunction = rowFunction;
        this.rs = rs;
        this.wrapper = new ForwardOnlyReadResultSet(rs);
    }

    /**
     * {@inheritDoc }
     *
     * @throws SQLDataAccessException if any java.sql.SQLException occurred
     * during the operation
     */
    @Override
    public boolean hasNext() throws SQLDataAccessException {
        try {
            if (hasNext == null) {
                hasNext = rs.next();
            }
            return hasNext;
        } catch (java.sql.SQLException e) {
            close();
            throw new SQLDataAccessException(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc }
     *
     * @throws SQLDataAccessException if any java.sql.SQLException occurred
     * during the operation
     */
    @Override
    public T next() throws SQLDataAccessException {
        try {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            try {
                return rowFunction.apply(wrapper);
            } catch (java.sql.SQLException | SQLDataAccessException ex) {
                close();
                throw new SQLDataAccessException(ex.getMessage(), ex);
            }
        } finally {
            hasNext = null;
        }
    }

    public void close() {
        if (rs != null) {
            if (LOG.isLoggable(Level.FINER)) {
                LOG.finer("Closing rs");
            }
            FunctionalSql.closeResources(rs);
        }
    }
}
