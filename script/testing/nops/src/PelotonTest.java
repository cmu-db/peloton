//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// PelotonTest.java
//
// Identification: script/testing/nops/src/PelotonTest.java
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
import java.util.Arrays;
import java.util.Random;
import java.util.Date;
import java.util.Properties;

public class PelotonTest {
  // Peloton, Postgres, Timesten
  private final String[] url = {
     "jdbc:postgresql://localhost:54321/postgres",  // PELOTON 
     "jdbc:postgresql://localhost:57721/postgres",   // POSTGRES
     "jdbc:timesten:client:TTC_SERVER_DSN=xxx;TTC_SERVER=xxx;TCP_PORT=xxx"}; // TIMESTEN

  private final String[] user = {"postgres", "postgres", "xxx"};
  private final String[] pass = {"postgres", "postgres", "xxx"};
  private final String[] driver = {"org.postgresql.Driver", "org.postgresql.Driver", "com.timesten.jdbc.TimesTenDriver"};
  
  private final int LOG_LEVEL = 0;
  private int query_type = SIMPLE_SELECT;

  public static final int SEMICOLON = 0;
  public static final int SIMPLE_SELECT = 1;

  public static final int PELOTON = 0;
  public static final int POSTGRES = 1;
  public static final int TIMESTEN = 2;



  private final String DROP = "DROP TABLE IF EXISTS A;" +
          "DROP TABLE IF EXISTS B;";
  private final String DDL = "CREATE TABLE A (id INT PRIMARY KEY, data TEXT);";

  private final String DATA_A = "1,'1961-06-16'";
  private final String DATA_B = "2,'Full Clip'";
  private final String INSERT_A = "INSERT INTO A VALUES ("+ DATA_A +");";
  private final String INSERT_B = "INSERT INTO A VALUES ("+ DATA_B +")";

  private final String INSERT_A_1 = "INSERT INTO A VALUES ("+ DATA_A +");";
  private final String INSERT_A_2 = "INSERT INTO A VALUES ("+ DATA_B +")";
  private final String DELETE_A = "DELETE FROM A WHERE id=1";

  private final String AGG_COUNT = "SELECT COUNT(*) FROM A";
  private final String AGG_COUNT_2 = "SELECT COUNT(*) FROM A WHERE id = 1";
  private final String AGG_COUNT_3 = "SELECT AVG(id) FROM A WHERE id < ? + 1";

  private final String TEMPLATE_FOR_BATCH_INSERT = "INSERT INTO A VALUES (?,?);";

  private final String INSERT = "BEGIN;" +
          "INSERT INTO A VALUES (1,'Fly High');" +
          "COMMIT;";

  private final String SEQSCAN = "SELECT * FROM A";
  private final String INDEXSCAN = "SELECT * FROM A WHERE id = 1";
  private final String INDEXSCAN_COLUMN = "SELECT data FROM A WHERE id = 1";
  private final String INDEXSCAN_PARAM = "SELECT * FROM A WHERE id = ?";
  private final String BITMAPSCAN = "SELECT * FROM A WHERE id > ? and id < ?";
  private final String UPDATE_BY_INDEXSCAN = "UPDATE A SET data=? WHERE id=?";
  private final String UPDATE_BY_SCANSCAN = "UPDATE A SET data='YO'";
  private final String DELETE_BY_INDEXSCAN = "DELETE FROM A WHERE id = ?";
  private final String SELECT_FOR_UPDATE = "SELECT * FROM A WHERE id = 1 FOR UPDATE";
  private final String UNION = "SELECT * FROM A WHERE id = ? UNION SELECT * FROM B WHERE id = ?";
  
  // To test the join in TPCC
  private final String CREATE_STOCK_TABLE = "CREATE TABLE STOCK ("
        + "S_W_ID INT PRIMARY KEY,"
        + "S_I_ID INT PRIMARY KEY,"
        + "S_QUANTITY DECIMAL NOT NULL);";
        //+ "PRIMARY KEY (S_W_ID, S_I_ID));";
  private final String CREATE_ORDER_LINE_TABLE = "CREATE TABLE ORDER_LINE ("
        + "OL_W_ID INT NOT NULL PRIMARY KEY,"
        + "OL_D_ID INT NOT NULL PRIMARY KEY,"
        + "OL_O_ID INT NOT NULL PRIMARY KEY,"
        + "OL_NUMBER INT NOT NULL PRIMARY KEY,"
        + "OL_I_ID INT NOT NULL);";
        //+ "PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER));";

  private final String INSERT_STOCK_1 = "INSERT INTO STOCK VALUES (1, 2, 0);";
  private final String INSERT_STOCK_2 = "INSERT INTO STOCK VALUES (1, 5, 1);";
  private final String INSERT_STOCK_3 = "INSERT INTO STOCK VALUES (1, 7, 6);";
  private final String SELECT_STOCK = "SELECT * FROM STOCK;";
  // Test general expression evaluation
  private final String SELECT_STOCK_COMPLEX = "SELECT * FROM STOCK WHERE"
      + " S_W_ID + S_I_ID = ? + S_QUANTITY + 1;";
  
  private final String INSERT_ORDER_LINE = "INSERT INTO ORDER_LINE VALUES (1, 2, 3, 4, 5);";
  private final String STOCK_LEVEL = "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT"
      + " FROM " + "ORDER_LINE, STOCK"
      //+ " FROM " + "ORDER_LINE JOIN STOCK on S_I_ID = OL_I_ID"
      + " WHERE OL_W_ID = ?"
      + " AND OL_D_ID = ?"
      + " AND OL_O_ID < ?"
      + " AND OL_O_ID >= ? - 20"
      + " AND S_W_ID = ?"
      + " AND S_I_ID = OL_I_ID"
      + " AND S_QUANTITY < ?";

  private final String CREATE_TIMESTAMP_TABLE = "CREATE TABLE TS ("
      + "id INT NOT NULL,"
      + "ts TIMESTAMP NOT NULL);";
  private final String INSERT_TIMESTAMP = "INSERT INTO TS VALUES (1, ?);";

  private final Connection conn;

  private enum TABLE {A, B, AB}

  private int target; 

  public PelotonTest(int target) throws SQLException {
    this.target = target;
    try {
      Class.forName(driver[target]);
      if (LOG_LEVEL != 0) {
        org.postgresql.Driver.setLogLevel(LOG_LEVEL);
        DriverManager.setLogStream(System.out);
      }
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    conn = this.makeConnection();
    return;
  }

  private Connection makeConnection() throws SQLException {
    Connection conn = DriverManager.getConnection(url[target], user[target], pass[target]);
    return conn;
  }

  public void Close() throws SQLException {
    conn.close();
  }

  /**
   * Drop if exists and create testing database
   *
   * @throws SQLException
   */
  public void Init() throws SQLException {
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement();
    if (target != TIMESTEN) {
      stmt.execute(DROP);
      stmt.execute(DDL);
    } else {
      try {
        stmt.execute("DROP TABLE A;");
      } catch (Exception e) {}
      stmt.execute("CREATE TABLE A(id INT, data INT);");
    } 
  }

  public void Nop_Test() throws Exception {
    long startTime = System.currentTimeMillis();
    long elapsedTime = 0L;
    long numOps = 1000 * 1000;
    if (query_type == SEMICOLON) {
      Statement stmt = conn.createStatement();
      for (long i = 0; i < numOps; i++) {
        try {
            stmt.execute(";");
        } catch (Exception e) { }
      } 
    } else {
      PreparedStatement stmt = conn.prepareStatement(INDEXSCAN_PARAM);
      conn.setAutoCommit(false);
      for (long i = 0; i < numOps; i++) {
         try {
            stmt.setInt(1, 0);
            stmt.execute();
            conn.commit();
         } catch (Exception e) { }
      }
    }
    elapsedTime = (new Date()).getTime() - startTime;
    System.out.println("Nop throughput: " + numOps * 1000 / elapsedTime);
  }


  static public void main(String[] args) throws Exception {
      if (args.length == 0) {
        System.out.println("Please specify target: [peloton|timesten|postgres]");
        return;
      }
      int target = PELOTON;
      if (args[0].equals("peloton")) {
        target = PELOTON;
      } else if (args[0].equals("postgres")) {
        target = POSTGRES;
        // echo "Please set env var LD_LIBRARY_PATH if you're using timesten. e.g. /home/rxian/TimesTen/ttdev2/lib"
      } else if (args[0].equals("timesten")) {
        target = TIMESTEN;
      } else {
        System.out.println("Please specify target: [peloton|timesten|postgres]");
        return;
      }
      PelotonTest pt = new PelotonTest(target);
      pt.Init();
      pt.Nop_Test();
      pt.Close();
  }

}
