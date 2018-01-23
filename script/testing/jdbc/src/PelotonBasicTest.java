//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// PelotonBasicTest.java
//
// Identification: script/testing/jdbc/src/PelotonBasicTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
import java.util.Arrays;
import java.util.Random;
import java.util.Date;
import java.util.Properties;

public class PelotonBasicTest {
  private final String url = "jdbc:postgresql://localhost:15721/";
  private final String username = "postgres";
  private final String pass = "postgres";
  private final int LOG_LEVEL = 0;

  private final int STAT_INTERVAL_MS = 1000;

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

  // To test the database stats colleced
  private final String SELECT_DB_METRIC = "SELECT * FROM pg_catalog.database_metric;";

  // To test the query stats colleced
  private final String SELECT_QUERY_METRIC = "SELECT * FROM pg_catalog.query_metric;";

  // To test the index stats colleced
  private final String SELECT_INDEX_METRIC = "SELECT * FROM pg_catalog.index_metric;";

  // To test the table stats colleced
  private final String SELECT_TABLE_METRIC = "SELECT * FROM pg_catalog.table_metric;";
  
  private final String INVALID_TABLE_SQL = "SELECT * FROM INVALID_TABLE;";

  private final String INVALID_DB_SQL = "SELECT * FROM INVALID_DB.A;";

  private final String INVALID_SYNTAX_SQL = "INVALIDKEYWORD * FROM A;";

  private final String UNSUPPORTED_JOIN_SQL = "SELECT * FROM A, B;";

  private final String UNSUPPORTED_INNER_JOIN_SQL = "SELECT * FROM A INNER JOIN B ON A.id = B.id;";

  private final String UNSUPPORTED_SELECT_FOR_UPDATE_SQL = "SELECT id FROM A FOR UPDATE where A.id = 1;";

  private final String INVALID_CREATE_SQL = "CREATE TABEL foo (id); ";

  private final String INVALID_PREPARE_SQL = "PREPARE func INSERT INTO foo(id int);";

  private final String EMPTY_SQL = ";;";

  public PelotonBasicTest() throws SQLException {
    try {
      Class.forName("org.postgresql.Driver");
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
    Connection conn = DriverManager.getConnection(url, username, pass);
    return conn;
  }

  public void Close() throws SQLException {
    System.out.println("Close called");
    conn.close();
  }

  /**
   * Drop if exists and create testing database
   *
   * @throws SQLException
   */
  public void Init() throws SQLException {
    System.out.println("Init");
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
    PreparedStatement pstmt = conn.prepareStatement(INSERT_TIMESTAMP);
    java.sql.Timestamp sysdate = new java.sql.Timestamp(
      System.currentTimeMillis());
    java.sql.Date today = new java.sql.Date(System.currentTimeMillis());
    Timestamp timestamp = new org.postgresql.util.PGTimestamp(System.currentTimeMillis());
    //pstmt.setDate(1, today);
    pstmt.setTimestamp(1, timestamp, null);
    //pstmt.setTimestamp(1, sysdate);
    //pstmt.setTimestamp(1, datetime, null);
    pstmt.execute();*/
    System.out.println("Test db created.");
    //System.exit(0);
  }

  public void ShowTable() throws SQLException {
    System.out.println("Show table");
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
    System.out.println("Scan Test");
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
   * Test SeqScan and IndexScan
   *
   * @throws SQLException
   */
  public void Stat_Test() throws Exception {
    System.out.println("Stat Test");
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    int txn_committed = 0;
    int table_update = 0;
    int table_delete = 0;
    int table_insert = 0;
    int table_read = 0;
    int index_read = 0;
    int index_insert = 0;

    // Test Insert
    stmt.execute(INSERT_A_1);
    txn_committed++;
    table_insert++;
    index_insert++;
    // Wait for stats to be aggregated
    Thread.sleep(STAT_INTERVAL_MS * 2);
    // Check query metric
    stmt.execute(SELECT_QUERY_METRIC);
    ResultSet rs = stmt.getResultSet();
    rs.last();
    int nums[] = new int[4];
    // Read
    nums[0] = rs.getInt(7);
    // Update
    nums[1] = rs.getInt(8);
    // Delete
    nums[2] = rs.getInt(9);
    // Insert
    nums[3] = rs.getInt(10);
    // Query
    String query = rs.getString(1);
    String row1 = query + "\tread:" + nums[0] + "\tupdate:" + nums[1]
                      + "\tdelete:" + nums[2] + "\tinsert:" + nums[3];
    System.out.println(row1);
    if (nums[3] != table_insert) {
      throw new SQLException("Query insert doesn't match!");
    }

    // Test Update
    UpdateByIndex(1);
    txn_committed++;
    table_update++;
    table_read++;
    index_read++;

    stmt.execute(INDEXSCAN);
    table_read++;
    txn_committed++;
    index_read++;

    stmt.execute(DELETE_A);
    table_read++;
    table_delete++;
    txn_committed++;
    index_read++;

    // Wait for stats to be aggregated
    Thread.sleep(STAT_INTERVAL_MS * 2);
    // Check index metric
    stmt.execute(SELECT_INDEX_METRIC);
    rs = stmt.getResultSet();
    rs.last();
    nums = new int[3];
    // Read
    nums[0] = rs.getInt(4);
    // Insert
    nums[2] = rs.getInt(6);
    if (nums[0] != index_read) {
      throw new SQLException("Index read doesn't match: " + nums[0]);
    }
    if (nums[2] != index_insert) {
      throw new SQLException("Index insert doesn't match: " + nums[2]);
    }


    // Check table metric
    stmt.execute(SELECT_TABLE_METRIC);
    rs = stmt.getResultSet();
    rs.last();
    nums = new int[4];
    // Read
    nums[0] = rs.getInt(3);
    // Update
    nums[1] = rs.getInt(4);
    // Delete
    nums[2] = rs.getInt(5);
    // Insert
    nums[3] = rs.getInt(6);
    if (nums[1] != table_update) {
      throw new SQLException("Table update doesn't match: " + nums[1]);
    }
    if (nums[2] != table_delete) {
      throw new SQLException("Table delete doesn't match: " + nums[2]);
    }
    if (nums[3] != table_insert) {
      throw new SQLException("Table insert doesn't match: " + nums[3]);
    }
    if (nums[0] != table_read) {
      throw new SQLException("Table read doesn't match: " + nums[0]);
    }

    // Check database metric
    stmt.execute(SELECT_DB_METRIC);
    rs = stmt.getResultSet();
    rs.last();
    nums = new int[2];
    // Commit
    nums[0] = rs.getInt(2);
    // Abort
    nums[1] = rs.getInt(3);
    if (nums[0] < txn_committed) {
      throw new SQLException("Txn committed count doesn't match!");
    }

    System.out.println("Stat test all good!");

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


  public void Batch_Insert() throws Exception{
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

  public void Copy_Test(String filePath) throws Exception{
    // Execute some queries
    PreparedStatement stmt = conn.prepareStatement(TEMPLATE_FOR_BATCH_INSERT);
    conn.setAutoCommit(true);
    for(int i=1; i <= 5 ;i++){
      stmt.setInt(1,i);
      stmt.setString(2,"Yo\nYo,Yo");
      stmt.addBatch();
    }

    int[] res;
    try{
       res = stmt.executeBatch();
    }catch(SQLException e){
      e.printStackTrace();
      throw e.getNextException();
    }
    
    // Wait for the query stat is flushed
    Thread.sleep(STAT_INTERVAL_MS * 2);
    
    // Perform Copy
    Statement copy_stmt = conn.createStatement();
    try {
    	copy_stmt.execute("COPY pg_catalog.query_metric TO '" + filePath + "' DELIMITER ','");
 	    ResultSet rs = copy_stmt.getResultSet();
    } catch (SQLException e) {
    	e.printStackTrace();
    }
    System.out.println("Copy finished");
  }

  public void Empty_SQL() throws SQLException {
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement();
    try {
      stmt.execute(EMPTY_SQL);
    } catch (Exception e) {
      throw new SQLException("Empty query should be valid");
    }
  }


  public void Invalid_SQL() throws SQLException {
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement();

    int validQueryIndex = -1;
    String statements[] = new String[8];
    statements[0] = INVALID_TABLE_SQL;
    statements[1] = INVALID_DB_SQL;
    statements[2] = INVALID_SYNTAX_SQL;
    statements[3] = UNSUPPORTED_JOIN_SQL;
    statements[4] = UNSUPPORTED_INNER_JOIN_SQL;
    statements[5] = UNSUPPORTED_SELECT_FOR_UPDATE_SQL;
    statements[6] = INVALID_CREATE_SQL;
    statements[7] = INVALID_PREPARE_SQL;
    for (int i = 0; i < 8; i++) {
      try {
        stmt.execute(statements[i]);
        validQueryIndex = i;
        break;
      } catch (Exception e) {
        // Great! Exception is expected!
      }
    }
    if (validQueryIndex != -1) {
      throw new SQLException("This should be an invalid SQL: " + statements[validQueryIndex]);
    }
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
  
  //tests inserting and reading back large binary values
  public void BlobTest() throws SQLException {
    System.out.println("Blob Test");
    Random rand = new Random(12345L);
    for (int i = 1; i < 10; i++) {
      Statement initstmt = conn.createStatement();
      initstmt.execute("DROP TABLE IF EXISTS A;");
      initstmt.execute("CREATE TABLE A (id INT PRIMARY KEY, data VARBINARY)");
      PreparedStatement stmt = conn.prepareStatement("INSERT INTO A VALUES (?,?);");
      
      System.out.println(i);
      int numbytes = 1 << i;
      byte[] sentbytes = new byte[numbytes];
      rand.nextBytes(sentbytes);
      stmt.setInt(1, 1);
      stmt.setBytes(2, sentbytes);
      stmt.execute();

      initstmt.execute(SEQSCAN);
      ResultSet rs = initstmt.getResultSet();
      if (!rs.next()) {
        System.err.println("failed at " + (numbytes) + " bytes");
        throw new SQLException("Did not get result after inserting into varbinary");
      }
      int recvint = rs.getInt(1);
      byte[] recvbytes = rs.getBytes(2);
      if (recvint != 1 || !Arrays.equals(sentbytes, recvbytes)) {
        System.err.println("failed at " + (numbytes) + " bytes");
        throw new SQLException(
            "byte mismatch: \n before: " + Arrays.toString(sentbytes) + "\n after:  " + Arrays.toString(recvbytes));
      }
      if (rs.next()) {
        System.err.println("failed at " + (numbytes) + " bytes");
        throw new SQLException("Too many results");
      }
    }
  }

  static public void main(String[] args) throws Exception {
      if (args.length < 1) {
        System.err.println("Please specify jdbc test target: [basic|stats|copy]");
      }
      if (args[0].equals("basic")) {
        BasicTest();
      } else if (args[0].equals("stats")) {
        StatsTest();
      } else if (args[0].equals("copy")) {
      	if (args.length < 2) {
	        System.err.println("Copy test usage: ./test_jdbc.sh copy [file_path]");
	    } else {
        	CopyTest(args[1]);
        }
      } else if (args[0].equals("simple")) {
      	SingleTest();
      }
  }

  static public void SingleTest() throws Exception {
    System.out.println("Basic Tests");
    PelotonBasicTest pt = new PelotonBasicTest();
    pt.Init();
//    pt.ShowTable();
    pt.Close(); 
  }

  static public void BasicTest() throws Exception {
    System.out.println("Basic Tests");
    PelotonBasicTest pt = new PelotonBasicTest();
    pt.Init();
    pt.ShowTable();
    pt.SeqScan();
    pt.Scan_Test();
    pt.Init();
    pt.Batch_Insert();
    pt.Batch_Update();
    pt.Batch_Delete();
    pt.Invalid_SQL();
    pt.Empty_SQL();
    pt.BlobTest();
    pt.Close();
  }

  static public void StatsTest() throws Exception {
    System.out.println("Stats Tests: make sure to start peloton with stats_mode 1");
    PelotonBasicTest pt = new PelotonBasicTest();
    pt.Init();
    pt.Stat_Test();
    pt.Close();
  }
 
  static public void CopyTest(String filePath) throws Exception {
    PelotonBasicTest pt = new PelotonBasicTest();
    pt.Init();
    pt.Copy_Test(filePath);
    pt.Close();
  }
}
