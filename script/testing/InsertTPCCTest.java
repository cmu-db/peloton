//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// InsertTPCCTest.java
//
// Identification: script/testing/junit/InsertTPCCTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
    
import org.junit.*;
import static org.junit.Assert.assertEquals;


public class InsertTPCCTest extends PLTestBase {
    private Connection conn;

    private static final String SQL_DROP_TABLE =
	"DROP TABLE IF EXISTS oorder;";

    private static final String SQL_CREATE_TABLE =
	"CREATE TABLE oorder (" +
	"o_w_id int NOT NULL PRIMARY KEY, " +
	"o_d_id int NOT NULL PRIMARY KEY, " +
	"o_id int NOT NULL PRIMARY KEY," +
	"o_c_id int NOT NULL," +
	"o_carrier_id int," +
	"o_ol_cnt decimal NOT NULL," +
	"o_all_local decimal NOT NULL," +
	"o_entry_d timestamp NOT NULL);";

    private void InitDatabase() throws SQLException {
	Statement stmt = conn.createStatement();
	stmt.execute(SQL_DROP_TABLE);
	stmt.execute(SQL_CREATE_TABLE);
    }

    @Before
    public void Setup() {
	try {
	    conn = makeDefaultConnection();
	    conn.setAutoCommit(true);
	    InitDatabase();
        } catch (SQLException ex) {
	    DumpSQLException(ex);
	    // throw ex;
	}
    }

    @After
    public void Teardown() throws SQLException {
	Statement stmt = conn.createStatement();
	stmt.execute(SQL_DROP_TABLE);
    }

    
    @Test
    public void testPstmtCVInsert() throws SQLException {
	int col;
	Timestamp ts = new org.postgresql.util.PGTimestamp((long) 100);
	
        String sql = "INSERT INTO oorder " +
	    "(O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)"+
	    " VALUES (?, ?, ?, ?, ?, ?, ?)";
	
        PreparedStatement pstmt = conn.prepareStatement(sql);
	col = 1;
	
	pstmt.setInt(col++, (int) 1);  //o_id
	pstmt.setInt(col++, (int) 2);  //o_d_id
	pstmt.setInt(col++, (int) 3);  //o_w_id
	pstmt.setInt(col++, (int) 4);  //o_c_id
	pstmt.setTimestamp(col++, ts); //o_entry_d
	pstmt.setInt(col++, (int) 6);  //o_ol_count
	pstmt.setInt(col++, (int) 7);  //o_all_local
	
	pstmt.addBatch();
	pstmt.executeBatch();

	String s_sql = "SELECT * FROM oorder;";
	PreparedStatement p_s_stmt = conn.prepareStatement(s_sql);
	ResultSet rs = p_s_stmt.executeQuery();

	rs.next();
	assertEquals(rs.getInt("o_id"), 1);
	assertEquals(rs.getInt("o_d_id"), 2);
	assertEquals(rs.getInt("o_w_id"), 3);
	assertEquals(rs.getInt("o_c_id"), 4);
	// skip o_entry_d timestamp
	assertEquals(rs.getInt("o_ol_cnt"), 6);
	assertEquals(rs.getInt("o_all_local"), 7);
	
	assertNoMoreRows(rs);
    }
}
