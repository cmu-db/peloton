//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// SequenceTest.java
//
// Identification: script/testing/junit/SequenceTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
import org.junit.*;
import org.postgresql.util.PSQLException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static junit.framework.TestCase.fail;

public class SequenceTest extends PLTestBase {
    private Connection conn1;
    private Connection conn2;

    private static final String SQL_DROP_SEQ =
            "DROP SEQUENCE seq;";

    private static final String SQL_CREATE_SEQ =
            "CREATE SEQUENCE seq;";

    private static final String SQL_NEXTVAL =
            "SELECT NEXTVAL('seq')";

    private static final String SQL_CURRVAL =
            "SELECT CURRVAL('seq')";

    /**
     * Test sequence functions for single-statment transactions
     */
    @Test
    public void test_SingleStmtTxn() throws SQLException {
        conn1 = makeDefaultConnection();
        conn1.setAutoCommit(true);
        Statement stmt1 = conn1.createStatement();

        conn2 = makeDefaultConnection();
        conn2.setAutoCommit(true);
        Statement stmt2 = conn2.createStatement();

        // Create a sequence
        stmt1.execute(SQL_CREATE_SEQ);
        
        // Check the sequence is visible by others 
        try {
            stmt2.execute(SQL_CREATE_SEQ);
            fail();
        } catch (PSQLException e) { }

        // Check currval cannot be called before nextval
        try {
            stmt1.execute(SQL_CURRVAL);
            fail();
        } catch (PSQLException e) { }

        // Check functionality with conn1
        stmt1.execute(SQL_NEXTVAL);
        ResultSet res1 = stmt1.executeQuery(SQL_CURRVAL);
        res1.next();
        assertEquals(1, res1.getInt(1));
        assertNoMoreRows(res1);

        // Update should be visible to conn2
        stmt2.execute(SQL_NEXTVAL);
        ResultSet res2 = stmt2.executeQuery(SQL_CURRVAL);
        res2.next();
        assertEquals(2, res2.getInt(1));
        assertNoMoreRows(res2);

        // Currval should be session consistent
        res1 = stmt1.executeQuery(SQL_CURRVAL);
        res1.next();
        assertEquals(1, res1.getInt(1));
        assertNoMoreRows(res1);

        // Clean up
        stmt1.close();
        conn1.close();
        stmt2.close();
        conn2.close();
    }

    /**
     * Test sequence functions for multi-statment transactions
     */
    @Test
    public void test_MultiStmtTxn() throws SQLException {
	    conn1 = makeDefaultConnection();
        conn1.setAutoCommit(false);
        Statement stmt1 = conn1.createStatement();

        conn2 = makeDefaultConnection();
        conn2.setAutoCommit(false);
        Statement stmt2 = conn2.createStatement();

        // Check functionality with conn1
        stmt1.execute(SQL_NEXTVAL);
        ResultSet res1 = stmt1.executeQuery(SQL_CURRVAL);
        res1.next();
        assertEquals(3, res1.getInt(1));
        assertNoMoreRows(res1);

        // Update should be visible to conn2
        stmt2.execute(SQL_NEXTVAL);
        ResultSet res2 = stmt2.executeQuery(SQL_CURRVAL);
        res2.next();
        assertEquals(4, res2.getInt(1));
        assertNoMoreRows(res2);

        // Rollback transactions
        conn1.rollback();
        conn2.rollback();

        // Check sequence incremental will not rollback
        conn1.setAutoCommit(true);
        stmt1.execute(SQL_NEXTVAL);
        res1 = stmt1.executeQuery(SQL_CURRVAL);
        res1.next();
        assertEquals(5, res1.getInt(1));
        assertNoMoreRows(res1);

        // Clean up
        stmt1.close();
        conn1.close();
        stmt2.close();
        conn2.close();
    }

    /**
     * Test dropping sequence
     */
    @Test
    public void test_Drop_Seq() throws SQLException {
        conn1 = makeDefaultConnection();
        conn1.setAutoCommit(true);
        Statement stmt1 = conn1.createStatement();

        conn2 = makeDefaultConnection();
        conn2.setAutoCommit(true);
        Statement stmt2 = conn2.createStatement();

        // Drop the sequence
        stmt1.execute(SQL_DROP_SEQ);

        // Check the sequence is invisible to all conns
        try {
            stmt1.execute(SQL_CURRVAL);
            fail();
        } catch (PSQLException e) { }
        try {
            stmt2.execute(SQL_CURRVAL);
            fail();
        } catch (PSQLException e) { }

        // Check the same sequence can be created w/o exception
        stmt2.execute(SQL_CREATE_SEQ);

        // Clean up
        stmt1.close();
        conn1.close();
        stmt2.close();
        conn2.close();
    }
}
