package org.example.storage.rdb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPool {
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;
    private static HikariDataSource dsTest;
    private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://csvc-db-2-instance-1.c7oym4pnfncu.us-east-1.rds.amazonaws.com:5432/postgres";
    private static final String USERNAME = "csvcadmin";
    private static final String PASSWORD = "!123Csvc";

    static {
        config.setJdbcUrl(CONNECTION_STRING);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("wrapperPlugins", "failover,efm");
        config.addDataSourceProperty("wrapperLogUnclosedConnections", "true");
//        config.addDataSourceProperty("maximumPoolSize", "3");
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(1);

        ds = new HikariDataSource(config);
//        dsTest = new HikariDataSource(config);
    }

    private ConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

//    public static Connection getTestConnection() throws SQLException {
//        return dsTest.getConnection();
//    }
}

/**
 * public class DataSource {
 * <p>
 * private static HikariConfig config = new HikariConfig();
 * private static HikariDataSource ds;
 * <p>
 * static {
 * config.setJdbcUrl( "jdbc_url" );
 * config.setUsername( "database_username" );
 * config.setPassword( "database_password" );
 * config.addDataSourceProperty( "cachePrepStmts" , "true" );
 * config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
 * config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
 * ds = new HikariDataSource( config );
 * }
 * <p>
 * private DataSource() {}
 * <p>
 * public static Connection getConnection() throws SQLException {
 * return ds.getConnection();
 * }
 * }
 */
