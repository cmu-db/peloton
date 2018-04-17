//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// UpdateTest.java
//
// Identification: script/testing/junit/UpdateTest.java
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

public class UpdateTest extends PLTestBase {
    private Connection conn;
    private String s_sql = "SELECT * FROM tbl;";    

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl (" +
	            "id integer, " +
                    "year integer);";

    /**
     * Initialize the database and table for testing
     */
    private void InitDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
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
     * Update columns in non-schema order. Exercises issue #1223.
     */
    // Disable until #1223 is fixed
    //@Test
    public void test_Update_1() throws SQLException {
	
        String sql_1 = "INSERT INTO tbl VALUES (5, 400);";	
        conn.createStatement().execute(sql_1);
	
        ResultSet rs_1 = conn.createStatement().executeQuery(s_sql);
        rs_1.next();
	checkRow(rs_1,
		 new String [] {"id", "year"},
		 new int [] {5, 400});
        assertNoMoreRows(rs_1);

        String sql_2 = "UPDATE tbl set year=year*2, id=id*2";
        conn.createStatement().execute(sql_2);
        ResultSet rs_2 = conn.createStatement().executeQuery(s_sql);

        rs_2.next();
	checkRow(rs_2,
		 new String [] {"id", "year"},
		 new int [] {10, 800});
        assertNoMoreRows(rs_2);

        String sql_3 = "UPDATE tbl set year=year*2, id=id*2";
        conn.createStatement().execute(sql_3);
        ResultSet rs_3 = conn.createStatement().executeQuery(s_sql);

        rs_3.next();
	checkRow(rs_3,
		 new String [] {"id", "year"},
		 new int [] {20, 1600});
        assertNoMoreRows(rs_3);
    }
}
