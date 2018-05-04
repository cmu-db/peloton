//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// InsertTest.java
//
// Identification: script/testing/junit/InsertTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * Insert statement tests.
 */

import java.sql.*;
import org.junit.*;
import static org.junit.Assert.assertEquals;

public class InsertTest extends PLTestBase {
    private Connection conn;
    private ResultSet rs;
    private String s_sql = "SELECT * FROM tbl;";    

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl (" +
                    "c1 int NOT NULL PRIMARY KEY, " +
                    "c2 int," +
                    "c3 int);";

    /**
     * Initialize the database and table for testing
     */
    private void InitDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
    }

    /**
     * Get columns, for follow on value checking
     */
    private void getResults() throws SQLException {
        Statement stmt = conn.createStatement();        
        rs = stmt.executeQuery(s_sql);
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
     * Insert statement tests
     * ---------------------------------------------
     */
    
    /**
     * 1 tuple insert, with no column specification.
     */
    @Test
    public void test_1Tuple_NCS() throws SQLException {
	
        String sql = "INSERT INTO tbl VALUES (1, 2, 3);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

	getResults();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /**
     * 1 tuple insert, with columns inserted in schema order.
     */
    @Test
    public void test_1Tuple_CS_1() throws SQLException {
	
        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (1, 2, 3);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

	getResults();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /**
     * 1 tuple insert, with columns inserted in different order from schema.
     */
    @Test
    public void test_1Tuple_CS_2() throws SQLException {
	
        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (3, 1, 2);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

	getResults();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /* 2 tuple inserts */
    
    /**
     * 2 tuple insert, with no column specification.
     */
    @Test
    public void test_2Tuple_NCS_1() throws SQLException {
	
        String sql = "INSERT INTO tbl VALUES (1, 2, 3), (11, 12, 13);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

	getResults();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {11, 12, 13});
        assertNoMoreRows(rs);
    }

    /**
     * 2 tuple insert, with no column specification, with fewer than
     * schema columns
     */
    //@Test
    public void test_2Tuple_NCS_2() throws SQLException {
	
        String sql = "INSERT INTO tbl VALUES (1), (11, 12);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

	getResults();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 0, 0});
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {11, 12, 0});
        assertNoMoreRows(rs);
    }
}
