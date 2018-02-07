//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// PelotonTest.java
//
// Identification: script/testing/jdbc/src/PelotonErrorTest.java
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;

public class PelotonErrorTest {
    private static final int NUM_TUPLES = 2;

    private static final String USERNAME = "postgres";
    private static final String PASS = "postgres";
    private static final int LOG_LEVEL = 0;

    private static final String DROP = "DROP TABLE IF EXISTS foo;";
    private static final String DDL = "CREATE TABLE foo(id integer PRIMARY KEY, year integer);";

    private static Connection makeConnection(String host, int port, String username, String pass) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/default_database", host, port);
        Connection conn = DriverManager.getConnection(url, username, pass);
        return conn;
    }

    /**
     * Drop if exists and create testing database
     * @throws SQLException
     */
    public static void initDB(Connection conn) throws SQLException {
        System.out.println("Init");
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        stmt.execute(DROP);
        stmt.execute(DDL);

        String sql = "INSERT INTO foo VALUES (?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 1; i <= NUM_TUPLES; i++) {
            pstmt.setInt(1, i);
            pstmt.setInt(2, i * 100);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
    }
    
    public static void ThreadOne(Connection conn, int targetId) throws SQLException {
        conn.setAutoCommit(false);
        String sql = "SELECT * FROM foo WHERE id = ? FOR UPDATE";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, targetId);
        ResultSet r = pstmt.executeQuery();
        if (r.next() == false) {
            throw new RuntimeException("[THREAD1] Failed to find id " + targetId);
        }
        
        // Then we hold....
        int sleep = 60;
        System.err.println("[THREAD1] Sleeping for " + sleep + " seconds");
        try {
            Thread.sleep(sleep * 1000);
        } catch (InterruptedException ex) {
            // ignore
        }
        
        // COMMIT!
        conn.commit();
    }
    
    public static void ThreadTwo(Connection conn, int targetId) throws Exception {
        int newYear = 999;
        String sql = "UPDATE foo SET year = ? WHERE id = ?";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        // pstmt.setQueryTimeout(5);

        // SLEEP!
        int sleep = 10;
        System.err.println("[THREAD2] Sleeping for " + sleep + " seconds");
        Thread.sleep(sleep * 1000);
        
        // UPDATE!
        conn.setAutoCommit(false);
        System.err.println("[THREAD2] Awake! Trying to execute query that updates tuple");
        
//        Statement setStmt = conn.createStatement();
//        boolean result = setStmt.execute("SET LOCAL statement_timeout TO 5");
//        if (result == false) {
//            System.err.println("[THREAD2] Failed to set 'statement_timeout'");
//            System.exit(1);
//        }
        
        try {
            pstmt.setInt(1, newYear);
            pstmt.setInt(2, targetId);
            pstmt.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
        conn.commit();
        
        // CHECK!
        sql = "SELECT * FROM foo WHERE id = ?";
        pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, targetId);
        ResultSet r = pstmt.executeQuery();
        if (r.next() == false) {
            throw new RuntimeException("[THREAD2] Failed to find id " + targetId);
        }
        if (r.getInt(2) != newYear) {
            throw new RuntimeException("[THREAD2] Failed to properly update id " + targetId + " [year=" + r.getInt(2) + "]");
        }
        r.close();
    }
    
    
    public static void DumpSQLException(SQLException ex) {
        System.err.println("Failed to execute test. Got " + ex.getClass().getSimpleName());
        System.err.println(" + Message:    " + ex.getMessage());
        System.err.println(" + Error Code: " + ex.getErrorCode());
        System.err.println(" + SQL State:  " + ex.getSQLState());
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("ARGS: [host] [port]");
            System.exit(1);
        }
//        System.err.println("ARGS => " + args);
//        System.err.println("ARG0: " + args[0]);
//        System.err.println("ARG1: " + args[1]);
        
        final String host = args[0];
        final int port = Integer.parseInt(args[1]); 

        final int targetId = 2;
        
        // Initialize the database!
        Class.forName("org.postgresql.Driver");
        if (LOG_LEVEL != 0) {
            org.postgresql.Driver.setLogLevel(LOG_LEVEL);
            DriverManager.setLogStream(System.out);
        }
        
        Thread t = null; 
        
        try {
            // Initialize the DB
            Connection conn = makeConnection(host, port, USERNAME, PASS);
            initDB(conn);
            
//            PreparedStatement pstmt = conn.prepareStatement("SET lock_timeout = 5");
//            boolean result = pstmt.execute();
//            if (result == false) {
//                System.err.println("Failed to set 'lock_timeout'");
//                pstmt.setQ
//                System.exit(1);
//            }
            
            // Start the first thread that holds the lock
            t = new Thread() {
                public void run() {
                    try {
                        Connection conn = makeConnection(host, port, USERNAME, PASS);
                        ThreadOne(conn, targetId);
                    } catch (SQLException ex) {
                        DumpSQLException(ex);
                    }
                };
            };
            t.start();
            
            // Then execute the second txn
            // This should throw the exception that we want.
            ThreadTwo(conn, targetId);
            
        } catch (SQLException ex) {
            DumpSQLException(ex);
            throw ex;
        } finally {
            if (t != null) {
                t.interrupt();
            }
        }
    }

}
