# sql-java
Having fun with functional SQL

## Example Class
```java
public class SomeClass {

    @Resource(name = "customerDB")
    private javax.sql.DataSource myDB;
    private final org.adeptnet.sql.FunctionalSql fsql = new org.adeptnet.sql.FunctionalSql(conSupplier());

    SQLSupplier<java.sql.Connection> conSupplier() {
        return () -> myDB.getConnection();
    }

    private void simpleStreamPrint(final String databaseName, final String tableName) {
        System.out.println(String.format("Database[%s] Table[%s]", databaseName, tableName));
    }

    private void simpleStream() throws SQLDataAccessException, SQLException {
        final String databaseName = "mysql";
        final String sqlString = String.format("SELECT `table_name` FROM `information_schema`.`tables` WHERE `table_schema` = '%s'", databaseName);
        try (final Stream<String> stream = fsql.stream(sqlString, row -> row.getString(1));) {
            stream.forEach(tableName -> simpleStreamPrint(databaseName, tableName));
        }
    }

    /*
     Same example as simpleStream, but with a prepare statement parameter query
     */
    private void simpleStreamParameter() throws SQLDataAccessException, SQLException {
        final String databaseName = "mysql";
        final String sqlString = "SELECT `table_name` FROM `information_schema`.`tables` WHERE `table_schema` = ?";
        try (final Stream<ResultSet> stream = fsql.streamFromParameterQuery(sqlString, databaseName);) {
            stream
                    .map(SQLFunction.checked(row -> row.getString(1)))
                    .forEach(tableName -> simpleStreamPrint(databaseName, tableName));
        }
    }

    /*
     Same example as simpleStream, but with a named sql parameter query
     */
    private void simpleStreamNamedParameter() throws SQLDataAccessException, SQLException {
        final String databaseName = "mysql";
        final String sqlString = "SELECT `table_name` FROM `information_schema`.`tables` WHERE `table_schema` = :databaseName";
        try (final Stream<ResultSet> stream = fsql.streamFromNamedParameterQuery(sqlString, java.util.Collections.singletonMap("databaseName", databaseName));) {
            stream
                    .map(SQLFunction.checked(row -> row.getString(1)))
                    .forEach(tableName -> simpleStreamPrint(databaseName, tableName));
        }
    }

    /*
     non resultset statement
     */
    private void simpleUpdate() throws SQLException {
        final String databaseName = "foo_db";
        final String sqlString = String.format("CREATE DATABASE IF NOT EXISTS `%s`", databaseName);
        fsql.execute(sqlString);
    }

    /*
    the real FUN
    */
    public List<User> simpleListUsers() throws SQLDataAccessException, SQLException {
        final String sqlString = "SELECT `user`.`User` userUser, `user`.`Host` userHost, `db`.* FROM `user` LEFT OUTER JOIN `db` ON `user`.`User` = `db`.`User` AND `user`.`Host` = `db`.`Host`";

        //Autoclosure of Streams incase of exceptions during operation
        try (final Stream<ResultSet> stream = fsql.stream(sqlString)) {
            return stream
                    .filter(SQLPredicate.checked((t) -> !"root@localhost".equalsIgnoreCase(String.format("%s@%s", t.getString("userUser"), t.getString("userHost")))))
                    .map(SQLFunction.checked((t) -> new User()
                                    .withUsername(t.getString("userUser"))
                                    .withHostname(t.getString("userHost"))
                                    .withDatabase(t.getString("Db"))
                            ))
                    .collect(Collectors.toList());
        }
    }

}
```
