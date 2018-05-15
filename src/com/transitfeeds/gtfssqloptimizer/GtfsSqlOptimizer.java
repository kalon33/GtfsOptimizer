package com.transitfeeds.gtfssqloptimizer;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class GtfsSqlOptimizer {

    /**
     * @param args
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        
        if (args.length < 1) {
            System.err.println("Usage: GtfsSqlOptimizer /path/to/db.sqlite");
            System.exit(1);
        }
        
        File file = new File(args[0]);
        
        if (!file.exists() || !file.isFile()) {
            System.err.println("Sqlite path must exist");
            System.err.println("Usage: GtfsSqlOptimizer /path/to/db.sqlite");
            System.exit(2);
        }
        
        Class.forName("org.sqlite.JDBC");

        String jdbcString = String.format("jdbc:sqlite:%s", file.getAbsolutePath()); 
        
        Connection connection = DriverManager.getConnection(jdbcString);
        
        Optimizer optimizer = new Optimizer(connection);
        optimizer.run();
    }

}
