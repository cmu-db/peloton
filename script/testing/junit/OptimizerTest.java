import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

import static java.sql.Statement.EXECUTE_FAILED;
import static java.sql.Statement.SUCCESS_NO_INFO;
import static org.junit.Assert.*;

/**
 * Created by Guoquan Zhao on 4/7/18.
 */
public class OptimizerTest extends PLTestBase {
    private static final String[] SQL_DROP_TABLES =
            {"DROP TABLE IF EXISTS t1;",
                    "DROP TABLE IF EXISTS t2;"};
    private Connection conn;

    private void initTables1() throws FileNotFoundException, SQLException {
        try (BufferedReader reader = new BufferedReader(new FileReader("create_tables_1.sql"));
             Statement stmt = conn.createStatement();) {
            reader.lines().forEach(s -> {
                try {
                    stmt.addBatch(s);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            int[] results = stmt.executeBatch();
            for (int i = 0; i < results.length; i++) {
                assertTrue("batch failed.", (results[i] >= 0 || results[i] == SUCCESS_NO_INFO) && results[i] != EXECUTE_FAILED);
            }
            ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM t1;");
            resultSet.next();
            assertEquals(3, resultSet.getInt(1));
            resultSet.close();
            resultSet = stmt.executeQuery("SELECT COUNT(*) FROM t2;");
            resultSet.next();
            assertEquals(3, resultSet.getInt(1));
            resultSet.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    @Before
    public void Setup() {
        try {
            conn = makeDefaultConnection();
            conn.setAutoCommit(true);
            initTables1();
        } catch (SQLException ex) {
            DumpSQLException(ex);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @After
    public void Teardown() throws SQLException {
        Statement stmt = conn.createStatement();
        for (String s : SQL_DROP_TABLES) {
            stmt.execute(s);
        }
    }


    @Test
    public void testInnerJoin() throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT t1.a FROM t1 INNER JOIN t2 ON (t1.b = t2.b) ORDER BY t1.a;");) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(2, resultSet.getInt(1));
            assertFalse(resultSet.next());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT x.a FROM t1 AS x INNER JOIN t2 ON(x.b = t2.b AND x.c = t2.c) ORDER BY x.a;");) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(2, resultSet.getInt(1));
            assertFalse(resultSet.next());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }


    }

    @Test
    public void testLeftOuterJoin() throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d;");) {
            assertTrue(resultSet.next());
            assertEquals(3, resultSet.getInt(4));
            assertEquals(4, resultSet.getInt(5));
            assertEquals(5, resultSet.getInt(6));
            assertEquals(1, resultSet.getInt(1));
            assertEquals(2, resultSet.getInt(2));
            assertEquals(3, resultSet.getInt(3));
            assertTrue(resultSet.next());
            assertEquals(null, resultSet.getObject(1));
            assertEquals(null, resultSet.getObject(2));
            assertEquals(null, resultSet.getObject(3));
            assertTrue(resultSet.next());
            assertEquals(null, resultSet.getObject(1));
            assertEquals(null, resultSet.getObject(2));
            assertEquals(null, resultSet.getObject(3));
            assertFalse(resultSet.next());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t1.a>1")) {
            assertTrue(resultSet.next());
            assertEquals(3, resultSet.getInt(4));
            assertEquals(4, resultSet.getInt(5));
            assertEquals(5, resultSet.getInt(6));
            assertEquals(1, resultSet.getInt(1));
            assertEquals(2, resultSet.getInt(2));
            assertEquals(3, resultSet.getInt(3));
            assertTrue(resultSet.next());
            assertEquals(null, resultSet.getObject(1));
            assertEquals(null, resultSet.getObject(2));
            assertEquals(null, resultSet.getObject(3));
            assertFalse(resultSet.next());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.a=t2.d WHERE t1.a>1")) {
            assertTrue(resultSet.next());
            assertEquals(3, resultSet.getInt(4));
            assertEquals(4, resultSet.getInt(5));
            assertEquals(5, resultSet.getInt(6));
            assertEquals(1, resultSet.getInt(1));
            assertEquals(2, resultSet.getInt(2));
            assertEquals(3, resultSet.getInt(3));
            assertTrue(resultSet.next());
            assertEquals(null, resultSet.getObject(1));
            assertEquals(null, resultSet.getObject(2));
            assertEquals(null, resultSet.getObject(3));
            assertFalse(resultSet.next());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }

    @Test
    public void testLeftOuterJoinWhere() {
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t2.b IS NULL OR t2.b>1")) {
            // expected result is
            // t1     t2
            // 1 2 3 {} {} {}
            // 2 3 4 {} {} {}
            assertTrue(resultSet.next());
            assertTrue(resultSet.next());
            assertFalse(resultSet.next());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }


}
