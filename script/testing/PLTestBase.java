//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// PLTestBase.java
//
// Identification: script/testing/junit/PLTestBase.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * Base class (helper functions) for prepared statement tests 
 */

import java.sql.*;
import org.junit.*;
import static org.junit.Assert.assertEquals;
    
public class PLTestBase {
    
    public static Connection makeDefaultConnection() throws SQLException {
	return makeConnection("localhost", 15721, "postgres", "postgres");
    }
    
    public static Connection makeConnection(String host,
					    int port,
					    String username,
					    String pass) throws SQLException {
	String url = String.format("jdbc:postgresql://%s:%d/default_database",
				   host, port);
	Connection conn = DriverManager.getConnection(url, username, pass);
	return conn;
    }

    /**
     * Assert that we have consumed all the rows.
     *
     * @param rs   resultset
     */
    public static void assertNoMoreRows(ResultSet rs) throws SQLException {
	int extra_rows = 0;
	while(rs.next()) {
	    extra_rows++;
	}
	assertEquals(extra_rows, 0);
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
	    assertEquals(expected_values[i], rs.getInt(columns[i]));
	}
    }
    
    
    public static void DumpSQLException(SQLException ex) {
	System.err.println("Failed to execute test. Got " +
			   ex.getClass().getSimpleName());
	System.err.println(" + Message:    " + ex.getMessage());
	System.err.println(" + Error Code: " + ex.getErrorCode());
	System.err.println(" + SQL State:  " + ex.getSQLState());
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
}
