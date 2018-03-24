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

    public static void assertNoMoreRows(ResultSet rs) throws SQLException {
	int extra_rows = 0;
	while(rs.next()) {
	    extra_rows++;
	}
	assertEquals(extra_rows, 0);
    }
    
    public static void DumpSQLException(SQLException ex) {
	System.err.println("Failed to execute test. Got " +
			   ex.getClass().getSimpleName());
	System.err.println(" + Message:    " + ex.getMessage());
	System.err.println(" + Error Code: " + ex.getErrorCode());
	System.err.println(" + SQL State:  " + ex.getSQLState());
    }
    
}
