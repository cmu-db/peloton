import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
            ExpectedResult expectedResult = new ExpectedResult("3");
            Utils.assertResultsSetEqual(resultSet, expectedResult);
            resultSet.close();
            resultSet = stmt.executeQuery("SELECT COUNT(*) FROM t2;");
            Utils.assertResultsSetEqual(resultSet, expectedResult);
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
            assertTrue(false);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            assertTrue(false);
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
    public void testInnerJoin1() throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT t1.a FROM t1 INNER JOIN t2 ON (t1.b = t2.b) ORDER BY t1.a;");) {
            ExpectedResult expectedResult = new ExpectedResult("1\n" +
                    "2");
            Utils.assertResultsSetEqual(resultSet, expectedResult);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testInnerJoin2() throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT x.a FROM t1 AS x INNER JOIN t2 ON(x.b = t2.b AND x.c = t2.c) ORDER BY x.a;");) {
            ExpectedResult expectedResult = new ExpectedResult("1\n" + "2");
            Utils.assertResultsSetEqual(resultSet, expectedResult);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testLeftOuterJoin1() throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d;");) {
            String r =
                    "1|2|3|3|4|5\n" +
                            "null|null|null|1|2|3\n" +
                            "null|null|null|2|3|4";
            ExpectedResult expectedResult = new ExpectedResult(r);
            Utils.assertResultsSetEqual(resultSet, expectedResult);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * There is an bug in the executor, skip this test for now.
     *
     * @throws SQLException
     */
    @Test
    @Ignore
    public void testLeftOuterJoin2() throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t1.a>1")) {
            String r =
                    "1|2|3|3|4|5\n" +
                            "null|null|null|2|3|4";
            ExpectedResult expectedResult = new ExpectedResult(r);
            Utils.assertResultsSetEqual(resultSet, expectedResult);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * There is an bug in the executor, skip this test for now.
     *
     * @throws SQLException
     */
    @Test
    @Ignore
    public void testLeftOuterJoin3() throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.a=t2.d WHERE t1.a>1")) {
            String r =
                    "1|2|3|3|4|5\n" +
                            "null|null|null|2|3|4";
            ExpectedResult expectedResult = new ExpectedResult(r);
            Utils.assertResultsSetEqual(resultSet, expectedResult);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * There is an bug in the executor, skip this test for now.
     *
     * @throws SQLException
     */
    @Test
    @Ignore
    public void testLeftOuterJoinWhere() {
        try (
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t2.b IS NULL OR t2.b>1")) {
            // expected result is
            // t1     t2
            // 1 2 3 {} {} {}
            // 2 3 4 {} {} {}
            String r =
                    "1|2|3|null|null|null\n" +
                            "2|3|4|null|null|null";
            ExpectedResult expectedResult = new ExpectedResult(r);
            Utils.assertResultsSetEqual(resultSet, expectedResult);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }


}
