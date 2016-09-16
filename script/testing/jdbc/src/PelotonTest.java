//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_plan.h
//
// Identification: script/testing/jdbc/src/PelotonTest.java
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
import org.postgresql.util.*;

public class PelotonTest {
  private final String url = "jdbc:postgresql://localhost:5432/";
  private final String username = "postgres";
  private final String pass = "postgres";

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

  ;

  public PelotonTest() throws SQLException {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    conn = this.makeConnection();
    return;
  }

  private Connection makeConnection() throws SQLException {
    Connection conn = DriverManager.getConnection(url, username, pass);
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
    stmt.execute(DROP);
    stmt.execute(DDL);

/*    stmt.execute(CREATE_STOCK_TABLE);
    stmt.execute(CREATE_ORDER_LINE_TABLE);
    stmt.execute(INSERT_STOCK_1);
    stmt.execute(SELECT_STOCK);
    stmt.execute(INSERT_STOCK_2);
    stmt.execute(SELECT_STOCK);
    stmt.execute(INSERT_STOCK_3);
    stmt.execute(SELECT_STOCK);
    stmt.execute(INSERT_ORDER_LINE);
    PreparedStatement pstmt = conn.prepareStatement(STOCK_LEVEL);
    pstmt.setInt(1, 1);
    pstmt.setInt(2, 2);
    pstmt.setInt(3, 5);
    pstmt.setInt(4, 20);
    pstmt.setInt(5, 1);
    pstmt.setDouble(6, 5);
    pstmt.execute();
    pstmt = conn.prepareStatement(SELECT_STOCK_COMPLEX);
    pstmt.setInt(1, 4);
    //pstmt.execute();
    ResultSet r=pstmt.executeQuery();

    while(r.next()) {
      System.out.println(r.getInt(1));
      System.out.println(r.getInt(2));
      System.out.println(r.getBigDecimal(3));
   }
   r.close();

    stmt.execute(CREATE_TIMESTAMP_TABLE);
    pstmt = conn.prepareStatement(INSERT_TIMESTAMP);
    java.sql.Timestamp sysdate = new java.sql.Timestamp(
      System.currentTimeMillis());
    java.sql.Date today = new java.sql.Date(System.currentTimeMillis());
    Timestamp timestamp = new org.postgresql.util.PGTimestamp(System.currentTimeMillis());
    //pstmt.setDate(1, today);
    pstmt.setTimestamp(1, timestamp, null);
    //pstmt.setTimestamp(1, sysdate);
    //pstmt.setTimestamp(1, datetime, null);
    pstmt.execute();
    System.out.println("Test db created.");
    //System.exit(0);*/
  }

  public void ShowTable() throws SQLException {
    int numRows;
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement();

    stmt.execute(SEQSCAN);

    ResultSet rs = stmt.getResultSet();

    ResultSetMetaData rsmd = rs.getMetaData();

    if (rsmd.getColumnCount() != 2) {
      throw new SQLException("Table should have 2 columns");
    } else if (rs.next()) {
      throw new SQLException("No rows should be returned");
    }
    System.out.println("Test db created.");
  }

  // TODO: remove this test - doesn't work on Postgres either.
  public void Multiple_Test() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("SELECT * FROM A WHERE id=?;SELECT * FROM A WHERE id=?");
    stmt.setInt(1, 1); 
    stmt.setInt(2, 1);
    ResultSet r = stmt.executeQuery(); 
    while (r.next()) {
      System.out.println("Multiple: id = " + r.getString(1) + ", " + r.getString(2));
    } 
  }

  /**
   * Test SeqScan and IndexScan
   *
   * @throws SQLException
   */
  public void Scan_Test() throws SQLException {
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement();
    stmt.execute(INSERT_A_1);
    stmt.execute(INSERT_A_2);
    stmt.execute(SEQSCAN);

    ResultSet rs = stmt.getResultSet();
    rs.next();
    String row1 = rs.getString(1) + "," + rs.getString(2);
    rs.next();
    String row2 = rs.getString(1) + "," + rs.getString(2);
    if (rs.next()) {
      throw new SQLException("More than 2 rows returned");
    } else if (row1.equals(row2)) {
      throw new SQLException("Rows aren't distinct");
    }

    stmt.execute(INDEXSCAN);
    stmt.execute(INDEXSCAN_COLUMN);

    // TODO: Aggregations not yet supported in Peloton
    stmt.execute(AGG_COUNT);
    stmt.execute(AGG_COUNT_2);
    PreparedStatement pstmt = conn.prepareStatement(AGG_COUNT_3);
    pstmt.setInt(1, 1);
    pstmt.execute();

    for (int i = 1; i < 3; i++)
        IndexScanParam(i);
    // TODO: Causes failed assertion on Peloton
    stmt.execute(DELETE_A);
    System.out.println("Scan test passed.");
  }

  /**
   * Insert a record
   *
   * @param n the id of the record
   * @throws SQLException
   */
  public void Insert(int n, TABLE table) throws SQLException {
    PreparedStatement stmt = null;
    switch (table) {
      case A:
        stmt = conn.prepareStatement(INSERT_A);
        break;
      case B:
        stmt = conn.prepareStatement(INSERT_B);
        break;
    }
    org.postgresql.PGStatement pgstmt = (org.postgresql.PGStatement) stmt;
    pgstmt.setPrepareThreshold(1);
    //System.out.println("Prepare threshold" + pgstmt.getPrepareThreshold());
    for (int i = 0; i < n; i++) {
      String data = table.name() + " says hello world and id = " + i;
      stmt.setInt(1, i);
      stmt.setString(2, data);
      boolean usingServerPrepare = pgstmt.isUseServerPrepare();
      int ret = stmt.executeUpdate();
      System.out.println("Used server side prepare " + usingServerPrepare + ", Inserted: " + ret);
    }
    return;
  }

  /**
   * Insert a record
   *
   * @param n the id of the record
   * @throws SQLException
   */
  public void Insert(int n) throws SQLException {
    conn.setAutoCommit(false);
    PreparedStatement stmt = conn.prepareStatement(INSERT);
    PreparedStatement stmtA = conn.prepareStatement(INSERT_A);
    PreparedStatement stmtB = conn.prepareStatement(INSERT_B);
    org.postgresql.PGStatement pgstmt = (org.postgresql.PGStatement) stmt;
    pgstmt.setPrepareThreshold(1);
    for (int i = 0; i < n; i++) {
      String data1 = "Ming says hello world and id = " + i;
      String data2 = "Joy says hello world and id = " + i;
      stmt.setInt(1, i);
      stmt.setInt(3, i);
      stmt.setString(2, data1);
      stmt.setString(4, data2);
      int ret = stmt.executeUpdate();
      /*
      String data = TABLE.A.name() + " says hello world and id = " + i;
      stmtA.setInt(1, i);
      stmtA.setString(2, data);
      data = TABLE.B.name();
      stmtB.setInt(1, i);
      stmtB.setString(2, data);
      int ret = stmtA.executeUpdate();
      System.out.println("A Used server side prepare " + pgstmt.isUseServerPrepare() + ", Inserted: " + ret);
      ret = stmtB.executeUpdate();
      System.out.println("B Used server side prepare " + pgstmt.isUseServerPrepare() + ", Inserted: " + ret);
      */
    }
    conn.commit();
    conn.setAutoCommit(true);
    return;
  }


  public void SeqScan() throws SQLException {
    System.out.println("\nSeqScan Test:");
    System.out.println("Query: " + SEQSCAN);
    PreparedStatement stmt = conn.prepareStatement(SEQSCAN);
    ResultSet r = stmt.executeQuery();
    while (r.next()) {
      System.out.println("SeqScan: id = " + r.getString(1) + ", " + r.getString(2));
    }
    r.close();
  }

  /**
   * Perform Index Scan with a simple equal qualifier
   *
   * @param i the param for the equal qualifier
   * @throws SQLException
   */
  public void IndexScanParam(int i) throws SQLException {
    System.out.println("\nIndexScan Test: ? = " + i);
    System.out.println("Query: " + INDEXSCAN_PARAM);
    PreparedStatement stmt = conn.prepareStatement(INDEXSCAN_PARAM);
    conn.setAutoCommit(false);
    stmt.setInt(1, i);
    stmt.execute();
    conn.commit();
    //ResultSet r = stmt.executeQuery();
    //while (r.next()) {
    //  System.out.println("IndexScanTest got tuple: id: " + r.getString(1) + ", data: " + r.getString(2));
    //}
    //r.close();
  }

  /**
   * Perform Index Scan with a simple equal qualifier
   *
   * @param i the param for the equal qualifier
   * @throws SQLException
   */
  public void BitmapScan(int i, int j) throws SQLException {
    System.out.println("\nBitmapScan Test: ? = " + i + ", " + j);
    System.out.println("Query: " + BITMAPSCAN);
    PreparedStatement stmt = conn.prepareStatement(BITMAPSCAN);
    stmt.setInt(1, i);
    stmt.setInt(2, j);
    ResultSet r = stmt.executeQuery();
    while (r.next()) {
      System.out.println("BitmapScanTest got tuple: id: " + r.getString(1) + ", data: " + r.getString(2));
    }
    r.close();
  }

  public void UpdateByIndex(int i) throws SQLException {
    System.out.println("\nUpdate Test: ? = " + i);
    System.out.println("Query: " + UPDATE_BY_INDEXSCAN);
    PreparedStatement stmt = conn.prepareStatement(UPDATE_BY_INDEXSCAN);
    stmt.setInt(2, i);
    stmt.setString(1, "Updated");
    stmt.executeUpdate();
    System.out.println("Updated: id: " + i + ", data: Updated");

  }


  public void UpdateBySeqScan() throws SQLException {
    System.out.println("\nUpdate Test: ");
    System.out.println("Query: " + UPDATE_BY_SCANSCAN);
    PreparedStatement stmt = conn.prepareStatement(UPDATE_BY_SCANSCAN);
    stmt.setString(1, "Updated");
    stmt.executeUpdate();
    System.out.println("Updated: data: Updated");

  }


  public void DeleteByIndexScan(int i) throws SQLException {
    System.out.println("\nDelete Test: ");
    System.out.println("Query: " + DELETE_BY_INDEXSCAN);
    PreparedStatement stmt = conn.prepareStatement(DELETE_BY_INDEXSCAN);
    stmt.setInt(1, i);
    stmt.executeUpdate();
    System.out.println("Deleted: id = " + i);
  }

  public void ReadModifyWrite(int i) throws SQLException {
    System.out.println("\nReadModifyWrite Test: ");
    System.out.println("Query: " + SELECT_FOR_UPDATE);
    PreparedStatement stmt = conn.prepareStatement(SELECT_FOR_UPDATE);

    stmt.setInt(1, i);
    ResultSet r = stmt.executeQuery();
    while (r.next()) {
      System.out.println("ReadModifyWriteTest got tuple: id: " + r.getString(1) + ", data: " + r.getString(2));
    }

    r.close();

    stmt = conn.prepareStatement(UPDATE_BY_INDEXSCAN);
    stmt.setInt(2, i);
    stmt.setString(1, "Updated");
    stmt.executeUpdate();

    System.out.println("Updated: id: " + i + ", data: Updated");
  }

  public void Union(int i) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(UNION);

    org.postgresql.PGStatement pgstmt = (org.postgresql.PGStatement) stmt;
    pgstmt.setPrepareThreshold(0);
    stmt.setInt(1, i);
    stmt.setInt(2, i);
    ResultSet r = stmt.executeQuery();

    System.out.println(r.getString(1));
  }


  public void Batch_Insert() throws SQLException{
    PreparedStatement stmt = conn.prepareStatement(TEMPLATE_FOR_BATCH_INSERT);

    conn.setAutoCommit(false);
    
    for(int i=1; i <= 5 ;i++){
      stmt.setInt(1,i);
      stmt.setString(2,"Yo");
      stmt.addBatch();
    }

    System.out.println("All Good Here");
    int[] res;
    try{
       res = stmt.executeBatch();
    }catch(SQLException e){
      e.printStackTrace();
      throw e.getNextException();
    }
    for(int i=0; i < res.length; i++){
      System.out.println(res[i]);
      if (res[i] < 0) {
        throw new SQLException("Query "+ (i+1) +" returned " + res[i]);
      }
    }
    conn.commit();
  }

  public void Batch_Update() throws SQLException{
    PreparedStatement stmt = conn.prepareStatement(UPDATE_BY_INDEXSCAN);

    conn.setAutoCommit(false);
    
    for(int i=1; i <= 3;i++){
      stmt.setString(1,"Cool");
      stmt.setInt(2,i);
      stmt.addBatch();
      }
      System.out.println("All Good Here");
      int[] res = stmt.executeBatch();
      for(int i=0; i < res.length; i++) {
        if (res[i] < 0) {
          throw new SQLException("Query "+ (i+1) +" returned " + res[i]);
        }
        System.out.println(res[i]);
      }
    conn.commit();
  }

  public void Batch_Delete() throws SQLException{
    PreparedStatement stmt = conn.prepareStatement(DELETE_BY_INDEXSCAN);

    conn.setAutoCommit(false);
    
    for(int i=1; i <= 3;i++){
      stmt.setInt(1,i);
      stmt.addBatch();
    }
    System.out.println("All Good Here");
    int[] res = stmt.executeBatch();
    for(int i=0; i < res.length; i++) {
      if (res[i] < 0) {
        throw new SQLException("Query "+ (i+1) +" returned " + res[i]);
      }
      System.out.println(res[i]);
    }
    conn.commit();
  }

  public void SelectParam() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(INDEXSCAN_PARAM);
    conn.setAutoCommit(false);
    stmt.setInt(1, 4);
    stmt.execute();
    conn.commit();
  }

  static public void main(String[] args) throws Exception {
    PelotonTest pt = new PelotonTest();
    pt.Init();
    pt.ShowTable();
    pt.SeqScan();
    pt.Scan_Test();
    pt.Init();
    pt.Batch_Insert();
    pt.Batch_Update();
    pt.Batch_Delete();
    pt.Close();
  }
}
