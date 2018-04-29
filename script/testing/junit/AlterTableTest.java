//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// AlterTableTest.java
//
// Identification: script/testing/junit/AlterTableTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;

public class AlterTableTest extends PLTestBase {
    private Connection conn;
    private Connection conn2;

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS foo;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE foo (" +
                    "id integer, " +
                    "year integer);";

    private static final String SQL_SELECT_STAR = "SELECT * FROM foo;";

    private static final String SQL_RENAME_COLUMN =
            "ALTER TABLE foo RENAME year to month;";


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Initialize the database and table for testing
     */
    private void InitDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);

        String sql = "INSERT INTO foo VALUES (5, 400);";
        conn.createStatement().execute(sql);
    }

    @Before
    public void Setup() throws SQLException {
        conn = makeDefaultConnection();
        conn.setAutoCommit(true);
        conn2 = makeDefaultConnection();
        InitDatabase();
    }

    @After
    public void Teardown() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
    }

    /**
     * Insert 1 tuple, rename the column, and see if the change is visible.
     */
    @Test
    public void test_RenameCol_Base() throws SQLException {
        conn.createStatement().execute(SQL_RENAME_COLUMN);
        ResultSet rs = conn.createStatement().executeQuery(SQL_SELECT_STAR);
        rs.next();
        checkRow(rs,
                new String [] {"id", "month"},
                new int [] {5, 400});
        assertNoMoreRows(rs);
    }

    /**
     * Rename a column that does not exists, should throw exception
     */
    @Test
    public void test_RenameCol_NotExist() throws SQLException {
        String sql = "ALTER TABLE foo RENAME a to b;";

        // Old column does not exist
        thrown.expect(PSQLException.class);
        conn.createStatement().execute(sql);
    }

    /**
     * Rename a column to a name that already exists, should throw exception
     */
    @Test
    public void test_RenameCol_Exist() throws SQLException {
        String sql = "ALTER TABLE foo RENAME year to id;";

        // New column already exists
        thrown.expect(PSQLException.class);
        conn.createStatement().execute(sql);
    }

    /**
     * Two transactions try to rename at the same time, should throw exception
     */
    @Test
    public void test_RenameCol_Concurrent() throws SQLException {
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);

        conn.createStatement().execute(SQL_RENAME_COLUMN);

        thrown.expect(PSQLException.class);
        conn2.createStatement().execute(SQL_RENAME_COLUMN);

        conn.commit();
        conn2.commit();
    }

//    The following tests are currently broken.
//
//    /**
//     * 2 transactions, t2 reads the table between t1 executes the rename
//     * and commits the changes.
//     */
//    @Test
//    public void test_RenameCol_ReadBeforeCommit() throws SQLException {
//        conn.setAutoCommit(false);
//        conn2.setAutoCommit(false);
//
//        String sql = "ALTER TABLE foo RENAME year to month;";
//        conn.createStatement().execute(sql);
//
//        ResultSet rs = conn2.createStatement().executeQuery(SQL_SELECT_STAR);
//        rs.next();
//        checkRow(rs,
//                new String [] {"id", "year"},
//                new int [] {5, 400});
//        assertNoMoreRows(rs);
//
//        conn.commit();
//        conn2.commit();
//    }
//
//    /**
//     * 2 transactions, t2 reads the table before and after t1 changes the
//     * column name and should not see the changes.
//     */
//    @Test
//    public void test_RenameCol_ReadBeforeAndAfterCommit() throws SQLException {
//        conn.setAutoCommit(false);
//        conn2.setAutoCommit(false);
//
//        ResultSet rs_1 = conn2.createStatement().executeQuery(SQL_SELECT_STAR);
//        rs_1.next();
//        checkRow(rs_1,
//                new String [] {"id", "year"},
//                new int [] {5, 400});
//        assertNoMoreRows(rs_1);
//
//        conn.createStatement().execute(SQL_RENAME_COLUMN);
//        conn.commit();
//
//        ResultSet rs_2 = conn2.createStatement().executeQuery(SQL_SELECT_STAR);
//        rs_2.next();
//        checkRow(rs_2,
//                new String [] {"id", "year"},
//                new int [] {5, 400});
//        assertNoMoreRows(rs_2);
//        conn2.commit();
//    }
//
//    /**
//     * 2 transactions, t2 reads the table after t1 commits the changes.
//     */
//    @Test
//    public void test_RenameCol_ReadAfterCommit() throws SQLException {
//        conn.setAutoCommit(false);
//        conn2.setAutoCommit(false);
//
//        Statement alter_statement = conn.createStatement();
//        Statement select_statement = conn2.createStatement();
//
//        alter_statement.execute(SQL_RENAME_COLUMN);
//        conn.commit();
//
//        ResultSet rs = select_statement.executeQuery(SQL_SELECT_STAR);
//        rs.next();
//        checkRow(rs,
//                new String [] {"id", "year"},
//                new int [] {5, 400});
//        assertNoMoreRows(rs);
//        conn2.commit();
//    }

}
