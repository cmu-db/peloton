//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// InsertPSTest.java
//
// Identification: script/testing/junit/InsertPSTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * 1 and 2 tuple, prepared insert statement tests.
 */

import java.sql.*;
import org.junit.*;
import static org.junit.Assert.assertEquals;

public class InsertPSTest extends PLTestBase {
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
     * Get columns, for follow on value checking, using a
     * prepared statement
     */
    private void getResultsPS() throws SQLException {
        rs = conn.prepareStatement(s_sql).executeQuery();
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

    /**
     * Prepared statement, 1 tuple insert, with no column specification.
     */
    @Test
    public void testPS_1Tuple_NCS() throws SQLException {
	
        String sql = "INSERT INTO tbl VALUES (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	setValues(pstmt, new int [] {1, 2, 3});
        pstmt.addBatch();
        pstmt.executeBatch();

        String s_sql = "SELECT * FROM tbl;";
        PreparedStatement p_s_stmt = conn.prepareStatement(s_sql);
        ResultSet rs = p_s_stmt.executeQuery();

        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /**
     * Prepared statement, 1 tuple insert, with columns inserted
     * in schema order.
     */
    @Test
    public void testPS_1Tuple_CS_1() throws SQLException {
	
        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	
	setValues(pstmt, new int [] {1, 2, 3});	
        pstmt.addBatch();
        pstmt.executeBatch();

	getResultsPS();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /**
     * Prepared statement, 1 tuple insert, with columns inserted
     * in different order from schema.
     */
    @Test
    public void testPS_1Tuple_CS_2() throws SQLException {
	
        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	
	setValues(pstmt, new int [] {3, 1, 2});		
        pstmt.addBatch();
        pstmt.executeBatch();

	getResultsPS();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /**
     * Prepared statement, 1 tuple insert, with columns inserted
     * in different order from schema, with one constant column.
     */
    @Test
    public void testPS_1Tuple_CS_3() throws SQLException {

        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (?, 1, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	setValues(pstmt, new int [] {3, 2});			

        pstmt.addBatch();
        pstmt.executeBatch();

	getResultsPS();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /**
     * Prepared statement, 1 tuple insert, with columns inserted
     * in schema order, with 2nd column missing.
     */
    @Test
    public void testPS_1Tuple_CS_4() throws SQLException {

        String sql = "INSERT INTO tbl (c1, c3) VALUES (?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	setValues(pstmt, new int [] {1, 3});	

        pstmt.addBatch();
        pstmt.executeBatch();

	getResultsPS();
	// shouldn't the default be null?
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 0, 3});
        assertNoMoreRows(rs);
    }

    /**
     * Prepared statement, 1 tuple insert, with columns inserted
     * in schema order, one constant column
     */
    @Test
    public void testPS_1Tuple_CS_5() throws SQLException {
	
        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (?, 2, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	
	setValues(pstmt, new int [] {1, 3});	
        pstmt.addBatch();
        pstmt.executeBatch();

	getResultsPS();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }
    
    /**
     * Prepared statement, 1 tuple insert, all constants
     */
    @Test
    public void testPS_1Tuple_CS_6() throws SQLException {
	
        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (1, 2, 3);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	
	// setValues(pstmt, new int [] {});
	// Todo: determine if this is 100% correct. addBatch call required
	// as, internally, SetParameterValues is where the constants
	// are inserted. 
        pstmt.addBatch();
        pstmt.executeBatch();

	getResultsPS();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }
    
    /* --------------------------------------------
     * 2 tuple insertions
    * ---------------------------------------------
    */

    /**
     * Prepared statement, 2 tuple insert, with no column specification.
     */
    @Test
    public void testPS_2Tuple_NCS() throws SQLException {
	
        String sql = "INSERT INTO tbl VALUES (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	setValues(pstmt, new int [] {1, 2, 3});	
        pstmt.addBatch();
	
	setValues(pstmt, new int [] {11, 12, 13});		
        pstmt.addBatch();	
        pstmt.executeBatch();

	getResultsPS();
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
     * Prepared statement, 2 tuple insert, with column inserted
     * in schema order
     * 
     */
    @Test
    public void testPS_2Tuple_CS_1() throws SQLException {
	
        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	setValues(pstmt, new int [] {1, 2, 3});	
        pstmt.addBatch();
	
	setValues(pstmt, new int [] {11, 12, 13});		
        pstmt.addBatch();	
        pstmt.executeBatch();

	getResultsPS();
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
     * Prepared statement, 2 tuple insert, with columns inserted
     * in different order from schema.
     */
    @Test
    public void testPS_2Tuple_CS_2() throws SQLException {
	
        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	
	setValues(pstmt, new int [] {3, 1, 2});
        pstmt.addBatch();
	
	setValues(pstmt, new int [] {13, 11, 12});
        pstmt.addBatch();
        pstmt.executeBatch();

	getResultsPS();
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
     * Prepared statement, 2 tuple insert, with columns inserted
     * in different order from schema, with one constant column.
     */
    @Test
    public void testPS_2Tuple_CS_3() throws SQLException {

        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (3, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	setValues(pstmt, new int [] {1, 2});
        pstmt.addBatch();
	
	setValues(pstmt, new int [] {11, 12});
        pstmt.addBatch();	
        pstmt.executeBatch();

	getResultsPS();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {11, 12, 3});
        assertNoMoreRows(rs);
    }

    /**
     * Prepared statement, 2 tuple insert, with columns inserted
     * in different order from schema, with one constant column.
     * Variant of above, with constant column last.
     */
    @Test
    public void testPS_2Tuple_CS_3a() throws SQLException {

        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (?, ?, 2);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	setValues(pstmt, new int [] {3, 1});
        pstmt.addBatch();
	
	setValues(pstmt, new int [] {13, 12});
        pstmt.addBatch();	
        pstmt.executeBatch();

	getResultsPS();
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 2, 3});
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {12, 2, 13});
        assertNoMoreRows(rs);
    }
    
    /**
     * Prepared statement, 2 tuple insert, with columns inserted
     * in schema order, with 2nd column missing.
     */
    @Test
    public void testPS_2Tuple_CS_4() throws SQLException {

        String sql = "INSERT INTO tbl (c1, c3) VALUES (?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	setValues(pstmt, new int [] {1, 3});
        pstmt.addBatch();
	
	setValues(pstmt, new int [] {11, 13});	
        pstmt.addBatch();
        pstmt.executeBatch();

	getResultsPS();
	// shouldn't the default be null?
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {1, 0, 3});
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c2", "c3"},
		 new int [] {11, 0, 13});
        assertNoMoreRows(rs);
    }
}
