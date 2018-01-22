//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// PelotonTest.java
//
// Identification: script/testing/jdbc/src/PelotonTypeTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PelotonTypeTest {
    private static final int NUM_TUPLES = 10;

    private static final String USERNAME = "postgres";
    private static final String PASS = "postgres";
    private static final int LOG_LEVEL = 0;

    private static final String SQL_DROP_TABLE = "DROP TABLE IF EXISTS foo;";
    private static final String SQL_CREATE_TABLE = 
            "CREATE TABLE foo (" +
            "  col1 TINYINT," +
            "  col2 SMALLINT," +
            "  col3 INTEGER," +
            "  col4 BIGINT" +
            ");";
    
    private static final List<long[]> EXPECTED = new ArrayList<long[]>();
    

    private static Connection makeConnection(String host, int port, String username, String pass) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/pavlo", host, port);
        Connection conn = DriverManager.getConnection(url, username, pass);
        return conn;
    }

    /**
     * Drop if exists and create testing database
     * @throws SQLException
     */
    public static void InitDatabase(Connection conn) throws SQLException {
        System.out.println("Init");
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);

        String sql = "INSERT INTO foo VALUES (?, ?, ?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 0; i < NUM_TUPLES; i++) {
            int col = 0;
            long row[] = {
                i,
                (i+i),
                (i * 1000),
                (i * 1000)
            };
            EXPECTED.add(row);
            
            // TINYINT
            pstmt.setByte(col+1,  (byte)row[col]);
            col++;
            
            // SMALLINT
            pstmt.setShort(col+1,  (short)row[col]);
            col++;
            
            // INTEGER
            pstmt.setInt(col+1,  (int)row[col]);
            col++;
            
            // BIGINT
            pstmt.setLong(col+1,  (long)row[col]);
            col++;
            
            pstmt.addBatch();
        }
        pstmt.executeBatch();
    }
    
    public static boolean CheckDatabase(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
        String sql = "SELECT * FROM foo ORDER BY col1";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet r = pstmt.executeQuery();
        
        int row = 0;
        boolean fail = false;
        while (r.next()) {
            long expected[] = EXPECTED.get(row);
            assert(expected != null);
            
            for (int col = 0; col < expected.length; col++) {
                // TINYINT
                if (col == 0) {
                    byte actual = r.getByte(col+1);
                    if (actual != (byte)expected[col]) {
                        System.err.println(
                                String.format("[%d] TINYINT FAIL: %d != %d",
                                        row, actual, expected[col]));
                        fail = true;
                    }
                // SMALLINT
                } else if (col == 1) {
                    short actual = r.getShort(col+1);
                    if (actual != (short)expected[col]) {
                        System.err.println(
                                String.format("[%d] SMALLINT FAIL: %d != %d",
                                        row, actual, expected[col]));
                        fail = true;
                    }
                // INTEGER
                } else if (col == 2) {
                    int actual = r.getInt(col+1);
                    if (actual != (int)expected[col]) {
                        System.err.println(
                                String.format("[%d] INTEGER FAIL: %d != %d",
                                        row, actual, expected[col]));
                        fail = true;
                    }
                // BIGINT
                } else if (col == 3) {
                    long actual = r.getLong(col+1);
                    if (actual != (long)expected[col]) {
                        System.err.println(
                                String.format("[%d] BIGINT FAIL: %d != %d",
                                        row, actual, expected[col]));
                        fail = true;
                    }
                }
            } // FOR
            row++;
        } // WHILE
        
        if (row != EXPECTED.size()) {
            throw new RuntimeException("Failed to retrieve all the rows from the database");
        }
        return (fail);
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
        final String host = args[0];
        final int port = Integer.parseInt(args[1]); 

        // Initialize the database!
        Class.forName("org.postgresql.Driver");
        if (LOG_LEVEL != 0) {
            org.postgresql.Driver.setLogLevel(LOG_LEVEL);
            DriverManager.setLogStream(System.out);
        }
        
        try {
            // Initialize the DB
            Connection conn = makeConnection(host, port, USERNAME, PASS);
            InitDatabase(conn);
            
            // Check that we get back the correct data
            CheckDatabase(conn);
            
        } catch (SQLException ex) {
            DumpSQLException(ex);
            throw ex;
        }
    }

}
