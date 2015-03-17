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

import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * Represents a supplier of results.
 *
 * <p>
 * There is no requirement that a new or distinct result be returned each time
 * the supplier is invoked.
 *
 * <p>
 * This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #get()}.
 *
 * Copied from java.util.function.Supplier
 *
 * @author Francois Steyn - Adept Internet (PTY) LTD (francois.s@adept.co.za)
 * @param <T> the type of results supplied by this supplier
 */
@FunctionalInterface
public interface SQLSupplier<T> {

    /**
     * Gets a result.
     *
     * @return a result
     * @throws java.sql.SQLException if underlying operation throws SQLException
     * @throws SQLDataAccessException if underlying operation throws
     * SQLDataAccessException
     */
    T get() throws java.sql.SQLException, SQLDataAccessException;

    /**
     *
     * @param <T> the type of result of the supplier
     * @param sqlSupplier SQLSupplier that will be wrapped
     * @return Supplier with possible SQLDataAccessException when SQLException
     * thrown by the SQLSupplier
     */
    static <T> Supplier<T> checked(SQLSupplier<T> sqlSupplier) {
        return () -> {
            try {
                return sqlSupplier.get();
            } catch (SQLException ex) {
                throw new SQLDataAccessException(ex.getMessage(), ex);
            }
        };
    }

}
