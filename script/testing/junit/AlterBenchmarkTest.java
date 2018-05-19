//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// AlterBenchmarkTest.java
//
// Identification: script/testing/junit/AlterBenchmarkTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;
import static org.junit.Assert.assertEquals;

/*
 * Test case that compare performance under different workload
 * Will need to contact with different local SQL
 */
public class AlterBenchmarkTest extends PLTestBase {
    private Connection conn;
    private Connection conn2;

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl (" +
                    "id integer, " +
                    "year integer," +
                    "month integer);";

    private static String selectSQL;
    private static String alterAdd;
    private static String alterDrop;
    private static String alterChangeInline;
    private static String alterChangeNotInline;

    private static enum DBMS {MySQL, Postgres, Peloton};
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

    public static Connection makePostgresConnection(String host,
                                            int port,
                                            String username,
                                            String pass) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/postgres", host, port);
        Connection conn = DriverManager.getConnection(url, username, pass);
        return conn;
    }

    private static Connection makeMySQLConnection(String host,
                                                  int port,
                                                  String username,
                                                  String pass) throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d/mysql", host, port);
        Connection conn = DriverManager.getConnection(url, username, pass);
        return conn;
    }

    /**
     * Setup the connection to peloton or other DBMS
     * If you want to run other
     * @throws SQLException
     */
    @Before
    public void Setup() throws SQLException {
        DBMS testingDB = DBMS.Peloton;
        String userName = "";
        String passWD = "";
        switch (testingDB){
            case Peloton:
                alterAdd = "alter table tbl add day integer;";
                alterDrop = "alter table tbl drop month;";
                alterChangeInline = "alter table tbl alter year type varchar";
                alterChangeNotInline = "alter table tbl alter year type integer USING year::INTEGER";
                conn = makeDefaultConnection();
                break;
            case Postgres:
                alterAdd = "alter table tbl add day integer;";
                alterDrop = "alter table tbl drop month;";
                alterChangeInline = "alter table tbl alter year type varchar";
                alterChangeNotInline = "alter table tbl alter year type integer USING year::INTEGER";
                conn = makePostgresConnection("localhost", 5432, userName, passWD);
                break;
            case MySQL:
                alterAdd = "alter table tbl add day integer;";
                alterDrop = "alter table tbl drop month;";
                alterChangeInline = "alter table tbl modify year type varchar";
                alterChangeNotInline = "alter table tbl modify year type integer";
                conn = makeMySQLConnection("localhost", 3306, userName, passWD);
                break;
        }
        conn.setAutoCommit(true);
        InitDatabase();
    }

    @After
    public void Teardown() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
    }

    /**
     * Insert workload{} tuples into the table
     * In order to test performancce variance under different workload
     * @throws SQLException
     */
    @Test
    public void test_tuple_number_varies() throws SQLException {
        //define tuple number
        int[] workload = {};
        for (int i = 0; i< workload.length;i++) {
            // firstly use select * to make sure all tuples are in memory
            // for postgres and other disk based DBMS
            InitDatabase();
            NumVarInsertHelper(workload[i]);
            String sql = "select * from tbl;";
            conn.createStatement().execute(sql);

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            String alterSql1 = alterAdd;
            long startTime1 = System.currentTimeMillis();
            conn.createStatement().execute(alterSql1);
            long endTime1 = System.currentTimeMillis();

            String alterSql2 = alterDrop;
            long startTime2 = System.currentTimeMillis();
            conn.createStatement().execute(alterSql2);
            long endTime2 = System.currentTimeMillis();

            String alterSql3 = alterChangeInline;
            long startTime3 = System.currentTimeMillis();
            conn.createStatement().execute(alterSql3);
            long endTime3 = System.currentTimeMillis();

            String alterSql4 = alterChangeNotInline;
            long startTime4 = System.currentTimeMillis();
            conn.createStatement().execute(alterSql4);
            long endTime4 = System.currentTimeMillis();

            System.out.println("Alter add column " + workload[i] + " tuples took: "
                    + (endTime1 - startTime1) + " milliseconds");
            System.out.println("Alter drop column " + workload[i] + " tuples took: "
                    + (endTime2 - startTime2) + " milliseconds");
            System.out.println("Alter change type from inline to not inline " + workload[i] + " tuples took: " +
                    (endTime3 - startTime3) + " milliseconds");
            System.out.println("Alter change type from not inline to inline " + workload[i] + " tuples took: " +
                    (endTime4 - startTime4) + " milliseconds");

        }
    }

    private void NumVarInsertHelper(int insertNum) throws SQLException {
        String sql;
        for (int i = 0; i < insertNum; i++) {
            sql = String.format("INSERT INTO tbl VALUES (%d, %d, %d);", i, i+1, i+2);
            conn.createStatement().execute(sql);
        }
    }

    /**
     * Insert 'tupleNum' tuple, and test performance under different
     * length of the tuple defined by workload{}
     * @throws SQLException
     */
    @Test
    public void test_tuple_length_variance() throws SQLException {
        //define tuple length
        int[] workload = {};
        int tupleNum = 10000;
        conn.createStatement().execute(SQL_DROP_TABLE);
        for (int i = 0; i < workload.length; i++) {
            String createSql = "CREATE TABLE tbl(id INTEGER PRIMARY KEY, " +
                    "month VARCHAR(" + workload[i] + ")," +
                    "hour VARCHAR(" + workload[i] + ")," +
                    "year INTEGER);";
            conn.createStatement().execute(createSql);
            LengthVarInsertHelper(tupleNum, workload[i]);

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            long startTime1 = System.currentTimeMillis();
            conn.createStatement().execute(alterAdd);
            long endTime1 = System.currentTimeMillis();

            long startTime2 = System.currentTimeMillis();
            conn.createStatement().execute(alterDrop);
            long endTime2 = System.currentTimeMillis();

            long startTime3 = System.currentTimeMillis();
            conn.createStatement().execute(alterChangeInline);
            long endTime3 = System.currentTimeMillis();

            System.out.println("Alter add column " + workload[i] * 2 +
                    " length took: " + (endTime1 - startTime1)
                    + " milliseconds");
            System.out.println("Alter drop column " + workload[i] * 2 +
                    " length took: " + (endTime2 - startTime2)
                    + " milliseconds");
            System.out.println("Alter change type from not inline to inline " + workload[i] * 2
                    + " length took: " + (endTime3 - startTime3) +
                    " milliseconds");

            conn.createStatement().execute(SQL_DROP_TABLE);
        }
    }

    // will simply generate string with length and return
    private String PayloadGenerate(int length) {
        long seed = System.currentTimeMillis() % 26 + 'a';
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append((char)seed);
        }
        return builder.toString();
    }

    private void LengthVarInsertHelper(int insertNum, int varLen) throws SQLException {
        String payload1 = PayloadGenerate(varLen);
        String payload2 = PayloadGenerate(varLen);
        for (int i = 0; i<insertNum; i++){
            if( i%1000 == 0) {
                payload1 = PayloadGenerate(varLen);
                payload2 = PayloadGenerate(varLen);
            }
            String sql = String.format("INSERT INTO tbl VALUES (%d, '%s', '%s', %d);", i, payload1, payload2, i+1);
            conn.createStatement().execute(sql);
        }
    }
}
