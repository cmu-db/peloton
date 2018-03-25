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
     * Set column values.
     *
     * @param pstmt   prepared statement to receive values
     * @param values  array of values
     */
    public void setValues(PreparedStatement pstmt,
			  int [] values) throws SQLException {
	int col = 1;
	for (int i=0; i<values.length; i++) {
	    pstmt.setInt(col++, (int) values[i]);
	}
    }

    /**
     * Check a single row of queried values against expected values
     *
     * @param rs              resultset, with cursor at the desired row
     * @param columns         column names
     * @param expected_values expected values of columns
     */

    public void checkRow(ResultSet rs,
			 String [] columns,
			 int [] expected_values) throws SQLException {
        assertEquals(columns.length, expected_values.length);
	for (int i=0; i<columns.length; i++) {
	    assertEquals(rs.getInt(columns[i]), expected_values[i]);
	}
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

    // Currently fails. See #1197
    //@Test
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

    // Currently fails. See #1197
    // @Test
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
    // Works, due to use of insert rather than push back

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
    // Currently fails. See #1197
    //@Test
    public void testPS_2Tuple_CS_2() throws SQLException {
	
        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	
	setValues(pstmt, new int [] {3, 2, 1});
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
    // Currently fails. See #1197
    //@Test
    public void testPS_2Tuple_CS_3() throws SQLException {

        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (?, 1, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
	setValues(pstmt, new int [] {3, 2});
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
		 new int [] {1, 12, 13});
        assertNoMoreRows(rs);
    }
    
    /**
     * Prepared statement, 2 tuple insert, with columns inserted
     * in schema order, with 2nd column missing.
     */
    // Currently failing. See comments in #1197
    // @Test
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
    

    /* --------------------------------------------
     * simple (non-prepared) insert statement tests
    * ---------------------------------------------
    */
    
    /**
     * 1 tuple insert, with no column specification.
     */
    //@Test
    public void test_1Tuple_NCS() throws SQLException {
	
        String sql = "INSERT INTO tbl VALUES (1, 2, 3);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

        String s_sql = "SELECT * FROM tbl;";
        Statement s_stmt = conn.createStatement();
        ResultSet rs = s_stmt.executeQuery(s_sql);
	
        rs.next();
        assertEquals(rs.getInt("c1"), 1);
        assertEquals(rs.getInt("c2"), 2);
        assertEquals(rs.getInt("c3"), 3);
        assertNoMoreRows(rs);
    }

    /**
     * 1 tuple insert, with columns inserted in schema order.
     */
    //@Test
    public void test_1Tuple_CS_1() throws SQLException {
	
        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (1, 2, 3);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

        String s_sql = "SELECT * FROM tbl;";
        Statement s_stmt = conn.createStatement();
        ResultSet rs = s_stmt.executeQuery(s_sql);

        rs.next();
        assertEquals(rs.getInt("c1"), 1);
        assertEquals(rs.getInt("c2"), 2);
        assertEquals(rs.getInt("c3"), 3);
        assertNoMoreRows(rs);
    }

    /**
     * 1 tuple insert, with columns inserted in different order from schema.
     */
    //@Test
    public void test_1Tuple_CS_2() throws SQLException {
	
        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (3, 1, 2);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

        String s_sql = "SELECT * FROM tbl;";
        Statement s_stmt = conn.createStatement();
        ResultSet rs = s_stmt.executeQuery(s_sql);

        rs.next();
        assertEquals(rs.getInt("c1"), 1);
        assertEquals(rs.getInt("c2"), 2);
        assertEquals(rs.getInt("c3"), 3);
        assertNoMoreRows(rs);
    }

    /* 2 tuple (non-prepared statement) inserts */
    
    /**
     * 2 tuple insert, with no column specification.
     */
    //@Test
    public void test_2Tuple_NCS() throws SQLException {
	
        String sql = "INSERT INTO tbl VALUES (1, 2, 3), (11, 12, 13);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

        String s_sql = "SELECT * FROM tbl;";
        Statement s_stmt = conn.createStatement();
        ResultSet rs = s_stmt.executeQuery(s_sql);

        rs.next();
        assertEquals(rs.getInt("c1"), 1);
        assertEquals(rs.getInt("c2"), 2);
        assertEquals(rs.getInt("c3"), 3);

        rs.next();
        assertEquals(rs.getInt("c1"), 11);
        assertEquals(rs.getInt("c2"), 12);
        assertEquals(rs.getInt("c3"), 13);
	
        assertNoMoreRows(rs);
    }
}
