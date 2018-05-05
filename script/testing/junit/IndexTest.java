//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// IndexTest.java
//
// Identification: script/testing/junit/IndexTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * Index concurrency tests.
 */

import java.sql.*;
import org.junit.*;
import static org.junit.Assert.assertEquals;

public class IndexTest extends PLTestBase {
    private Connection conn;

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl(c1 int, c2 int);";

    /**
     * Initialize the database and table for testing
     */
    private void InitDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
        for (int i = 0; i < 10000; i++) {
            stmt.execute("INSERT INTO tbl VALUES (" + i + ", " + i + ");");
        }
    }
    
    @Before
    public void Setup() throws SQLException {
	    conn = makeDefaultConnection();
	    conn.setAutoCommit(true);
	    InitDatabase();
    }

    @After
    public void Teardown() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
    }

    /* --------------------------------------------
     * Index concurrency tests
     * ---------------------------------------------
     */
    
    /**
     * 1 thread create index, 1 thread insert tuples
     */
    @Test
    public void test_create_insert() throws SQLException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                  Statement stmt = conn.createStatement();
                  stmt.execute("CREATE INDEX i1 ON tbl(c1);");
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                  Statement stmt = conn.createStatement();
                  stmt.execute("INSERT INTO tbl VALUES(-1, -1);");
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();
	
	    Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 0;");
        rs.next();
	    checkRow(rs,
		 new String [] {"c1", "c2"},
		 new int [] {-1, -1});
        assertNoMoreRows(rs);
    }

    /**
     * 1 thread create index, 1 thread update tuples
     */
    @Test
    public void test_1Tuple_CS_1() throws SQLException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                  Statement stmt = conn.createStatement();
                  stmt.execute("CREATE INDEX i1 ON tbl(c1);");
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                  Statement stmt = conn.createStatement();
                  stmt.execute("UPDATE tbl SET c1 = -1 WHERE c1 = 0;");
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 0;");
        rs.next();
        checkRow(rs,
         new String [] {"c1", "c2"},
         new int [] {-1, 0});
        assertNoMoreRows(rs);
    }

    /**
     * 1 thread create index, 1 thread delete tuples
     */
    @Test
    public void test_1Tuple_CS_2() throws SQLException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                  Statement stmt = conn.createStatement();
                  stmt.execute("CREATE INDEX i1 ON tbl(c1);");
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                  Statement stmt = conn.createStatement();
                  stmt.execute("DELETE FROM tbl WHERE c1 = 0;");
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 2;");
        rs.next();
        checkRow(rs,
         new String [] {"c1", "c2"},
         new int [] {1, 1});
        assertNoMoreRows(rs);
    }

}
