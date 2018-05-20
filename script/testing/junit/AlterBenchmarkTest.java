//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// AlterTableTest.java
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
        String url = String.format("jdbc:postgresql://%s:%d/postgres",
                host, port);
        Connection conn = DriverManager.getConnection(url, username, pass);
        return conn;
    }

    /**
     * Setup the connection to peloton or other DBMS
     * @throws SQLException
     */
    @Before
    public void Setup() throws SQLException {
        //connection to Postgres
        //conn = makePostgresConnection("localhost", 5432, "dingshilun", "");
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
     * Insert workload{} tuples into the table
     * In order to test performancce variance under different workload
     * @throws SQLException
     */
    @Test
    public void test_tuple_number_varies() throws SQLException {
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
            
            String alterSql1 = "alter table tbl add day integer;";
            long startTime1 = System.currentTimeMillis();
            conn.createStatement().execute(alterSql1);
            long endTime1 = System.currentTimeMillis();

            String alterSql2 = "alter table tbl drop month;";
            long startTime2 = System.currentTimeMillis();
            conn.createStatement().execute(alterSql2);
            long endTime2 = System.currentTimeMillis();

            String alterSql3 = "alter table tbl alter year type varchar";
            long startTime3 = System.currentTimeMillis();
            conn.createStatement().execute(alterSql3);
            long endTime3 = System.currentTimeMillis();

            String alterSql4 = "alter table tbl alter year type integer USING year::INTEGER";
            long startTime4 = System.currentTimeMillis();
            conn.createStatement().execute(alterSql4);
            long endTime4 = System.currentTimeMillis();

            System.out.println("Alter add column " + workload[i] + " tuples took: " + (endTime1 - startTime1) + " milliseconds");
            System.out.println("Alter drop column " + workload[i] + " tuples took: " + (endTime2 - startTime2) + " milliseconds");
            System.out.println("Alter change type from inline to not inline " + workload[i] + " tuples took: " +
                    (endTime3 - startTime3) + " milliseconds");
            System.out.println("Alter change type from not inline to inline " + workload[i] + " tuples took: " +
                    (endTime4 - startTime4) + " milliseconds");

        }
    }

    private void NumVarInsertHelper(int insertNum) throws SQLException {
        String sql = "INSERT INTO tbl VALUES (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 0; i < insertNum; i++) {
            setValues(pstmt, new int [] {i, i+1, i+2});
            pstmt.addBatch();
        }
        pstmt.executeBatch();
    }

    /**
     * Insert 10000 tuple, and test performance under different
     * length of the tuple
     * @throws SQLException
     */
    @Test
    public void test_tuple_length_variance() throws SQLException {
        int[] workload = {};
        int tupleNum = 10000;
        String dropSQL = "DROP TABLE IF EXISTS tbl";
        String sql = "";
        conn.createStatement().execute(dropSQL);
        for (int i = 0; i < workload.length; i++) {
            sql = "CREATE TABLE tbl(id INTEGER PRIMARY KEY, " +
                    "payload1 VARCHAR(" + workload[i] + ")," +
                    "payload2 VARCHAR(" + workload[i] + ")," +
                    "payload3 INTEGER);";
            conn.createStatement().execute(sql);
            LengthVarInsertHelper(tupleNum, workload[i]);

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            long startTime1 = System.currentTimeMillis();
            conn.createStatement().execute("ALTER TABLE tbl add payload4 integer;");
            long endTime1 = System.currentTimeMillis();

            long startTime2 = System.currentTimeMillis();
            conn.createStatement().execute("ALTER TABLE tbl drop payload1;");
            long endTime2 = System.currentTimeMillis();

            long startTime3 = System.currentTimeMillis();
            conn.createStatement().execute("ALTER TABLE tbl alter payload3 type varchar;");
            long endTime3 = System.currentTimeMillis();

            System.out.println("Alter add column " + workload[i] + " length took: " + (endTime1 - startTime1)
                    + " milliseconds");
            System.out.println("Alter drop column " + workload[i] + " length took: " + (endTime2 - startTime2)
                    + " milliseconds");
            System.out.println("Alter change type from not inline to inline " + workload[i] + " length took: " +
                    (endTime3 - startTime3) + " milliseconds");

            conn.createStatement().execute(dropSQL);
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
