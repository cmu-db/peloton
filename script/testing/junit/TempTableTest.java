//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// TempTableTest.java
//
// Identification: script/testing/junit/TempTableTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
import org.junit.*;
import org.postgresql.util.PSQLException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TempTableTest extends PLTestBase {

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    // Create a permanent table called "tbl"
    private static final String SQL_CREATE_PERM_TABLE =
            "CREATE TABLE tbl (id integer, year integer);";
    // Create temp table using "TEMP" keyword
    private static final String SQL_CREATE_TEMP_TABLE =
            "CREATE TEMP TABLE tbl (id integer, year integer);";
    // Create temp table using "TEMPORARY" keyword
    private static final String SQL_CREATE_TEMPORARY_TABLE =
            "CREATE TEMPORARY TABLE tbl (id integer, year integer);";

    private static final String SQL_INSERT =
            "INSERT INTO tbl VALUES (10, 1995);";

    private static final String SQL_SELECT = "SELECT * FROM tbl;";
            
    /**
     * Check the temp table created by one session is visible to itself, 
     * but not visible to other sessions
     */
    @Test
    public void test_Visibility() throws SQLException {
        Connection conn1 = makeDefaultConnection();
        conn1.setAutoCommit(true);
        Statement stmt1 = conn1.createStatement();

        Connection conn2 = makeDefaultConnection();
        conn2.setAutoCommit(true);
        Statement stmt2 = conn2.createStatement();
        
        // Part 1: Create temp table using "TEMP" keyword
        stmt1.execute(SQL_DROP_TABLE);
        // Create a temp table called "tbl"
        stmt1.execute(SQL_CREATE_TEMP_TABLE);
        // Insert a tuple into the temp table
        stmt1.execute(SQL_INSERT);
        
        // Check the temp table is visible to the session created it
        ResultSet res1 = stmt1.executeQuery(SQL_SELECT);
        res1.next();
        checkRow(res1,
            new String [] {"id", "year"},
            new int [] {10, 1995});
        assertNoMoreRows(res1);

        // Check the temp table is invisible to another session started 
        // before the temp table was created
        // Expect an exception: "Table tbl is not found"
        try {
            stmt2.execute(SQL_SELECT);
            fail();
        } catch (PSQLException e) { }

        // Check the temp table is invisible to another session started 
        // after the temp table was created
        // Expect an exception: "Table tbl is not found"
        conn2 = makeDefaultConnection();
        conn2.setAutoCommit(true);
        stmt2 = conn2.createStatement();
        try {
            stmt2.execute(SQL_SELECT);
            fail();
        } catch (PSQLException e) { }
        stmt2.close();
        conn2.close();

        // Check the temp table is invisible to another session started 
        // after the session which created it has closed
        // Expect an exception: "Table tbl is not found"
        stmt1.close();
        conn1.close();
        conn2 = makeDefaultConnection();
        conn2.setAutoCommit(true);
        stmt2 = conn2.createStatement();
        try {
            stmt2.execute(SQL_SELECT);
            fail();
        } catch (PSQLException e) { }
        stmt2.close();
        conn2.close();

        // Part 2: Create temp table using "TEMPORARY" keyword
        conn1 = makeDefaultConnection();
        conn1.setAutoCommit(true);
        stmt1 = conn1.createStatement();
        stmt1.execute(SQL_DROP_TABLE);
        stmt1.execute(SQL_CREATE_TEMPORARY_TABLE);

        // Check the temp table is visible to the session created it
        ResultSet res2 = stmt1.executeQuery(SQL_SELECT);
        res2.next();
        assertNoMoreRows(res2);
        
        // Check the temp table is invisible to another session started 
        // before the table was created
        // Expect an exception: "Table tbl is not found"
        try {
            stmt2.execute(SQL_SELECT);
            fail();
        } catch (PSQLException e) { }

        stmt1.close();
        conn1.close();
        stmt2.close();
        conn2.close();
    }

    /**
     * Check that during the lifetime of a temp table, the permanent table 
     * with the same name is invisible
     */
    @Test
    public void test_Temp_Table_Hides_Perm_Table() throws SQLException {
        Connection conn = makeDefaultConnection();
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();

        stmt.execute(SQL_DROP_TABLE);
        // Create a permanent table called "tbl"
        stmt.execute(SQL_CREATE_PERM_TABLE);
        // Create a temp table called "tbl"
        stmt.execute(SQL_CREATE_TEMP_TABLE);
        // Insert a tuple into the temp table
        stmt.execute(SQL_INSERT);

        // Check the "tbl" visible now is the temp table, not the permanent table
        ResultSet res1 = stmt.executeQuery(SQL_SELECT);
        res1.next();
        checkRow(res1,
            new String [] {"id", "year"},
            new int [] {10, 1995});
        assertNoMoreRows(res1);

        // Drop the temp table
        stmt.execute(SQL_DROP_TABLE);

        // Check the "tbl" visible now is the permanent table
        ResultSet res2 = stmt.executeQuery(SQL_SELECT);
        res2.next();
        assertNoMoreRows(res2);

        // Drop the permanent table
        stmt.execute(SQL_DROP_TABLE);

        // No table named "tbl" should exist now
        // Expect an exception: "Table tbl is not found"
        try {
            stmt.execute(SQL_SELECT);
            fail();
        } catch (PSQLException e) { }

        stmt.close();
        conn.close();
    }
}
