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
    private String s_sql = "SELECT * FROM foo;";

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS foo;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE foo (" +
                    "id integer, " +
                    "year integer);";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
     * Insert 1 tuple, rename the column, and see if the change is visible.
     */
    @Test
    public void test_RenameColumn_1() throws SQLException {
        String sql_1 = "INSERT INTO foo VALUES (5, 400);";
        conn.createStatement().execute(sql_1);

        ResultSet rs_1 = conn.createStatement().executeQuery(s_sql);
        rs_1.next();
        checkRow(rs_1,
                new String [] {"id", "year"},
                new int [] {5, 400});
        assertNoMoreRows(rs_1);

        String sql_2 = "ALTER TABLE foo RENAME year to month;";
        conn.createStatement().execute(sql_2);
        ResultSet rs_2 = conn.createStatement().executeQuery(s_sql);

        rs_2.next();
        checkRow(rs_2,
                new String [] {"id", "month"},
                new int [] {5, 400});
        assertNoMoreRows(rs_2);
    }

    /**
     * Rename a column that does not exist, should throw exception
     */
    @Test
    public void test_RenameColumn_2() throws SQLException {
        String sql = "ALTER TABLE foo RENAME a to b;";

        // Old column does not exist
        thrown.expect(PSQLException.class);
        conn.createStatement().execute(sql);
    }

    /**
     * Rename a column to a name that already exists, should throw exception
     */
    @Test
    public void test_RenameColumn_3() throws SQLException {
        String sql = "ALTER TABLE foo RENAME year to id;";

        // New column already exists
        thrown.expect(PSQLException.class);
        conn.createStatement().execute(sql);
    }

}
