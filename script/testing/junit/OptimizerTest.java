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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Guoquan Zhao on 4/7/18.
 */
public class OptimizerTest extends PLTestBase {
    private static final String[] SQL_DROP_TABLES =
            {"DROP TABLE IF EXISTS t1;",
                    "DROP TABLE IF EXISTS t2;"};
    private Connection conn;
    private void initTables1() throws FileNotFoundException, SQLException {
        try(BufferedReader reader = new BufferedReader(new FileReader("create_tables_1.sql"));
            Statement stmt = conn.createStatement();){
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
            assertEquals(3, resultSet.getInt(0));
            resultSet.close();
            resultSet = stmt.executeQuery("SELECT COUNT(*) FROM t2;");
            assertEquals(3, resultSet.getInt(0));
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
            // throw ex;
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
    public void testJoin1() throws SQLException {


    }

}
