import java.sql.*;

/**
 * Author:  Ming Fang
 * Date:    7/19/15.
 * Email:   mingf@cs.cmu.edu
 */
public class PelotonTest {
  private final String url = "jdbc:postgresql://localhost:57721/";
  private final String username = "postgres";
  private final String pass = "postgres";

  private final String DROP = "DROP TABLE IF EXISTS A;" +
          "DROP TABLE IF EXISTS B;";
  private final String DDL = "CREATE TABLE A (id INT PRIMARY KEY, data TEXT);" +
          "CREATE TABLE B (id INT PRIMARY KEY, data TEXT);";

  private final String INSERT_A = "INSERT INTO A VALUES (?,?)";
  private final String INSERT_B = "INSERT INTO B VALUES (?,?)";

  private final String INSERT = "BEGIN;" +
          "INSERT INTO A VALUES (?,?);" +
          "INSERT INTO B VALUES (?,?);" +
          "COMMIT;";

  private final String SEQSCAN = "SELECT * FROM A";
  private final String INDEXSCAN = "SELECT * FROM A WHERE id = ?";
  private final String BITMAPSCAN = "SELECT * FROM A WHERE id > ? and id < ?";
  private final String UPDATE_BY_INDEXSCAN = "UPDATE A SET data=? WHERE id=?";
  private final String UPDATE_BY_SCANSCAN = "UPDATE A SET data=?";
  private final String DELETE_BY_INDEXSCAN = "DELETE FROM A WHERE id = ?";
  private final String SELECT_FOR_UPDATE = "SELECT * FROM A WHERE id = ? FOR UPDATE";
  private final String UNION = "SELECT * FROM A WHERE id = ? UNION SELECT * FROM B WHERE id = ?";

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
    System.out.println("Test db created.");
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
  public void IndexScan(int i) throws SQLException {
    System.out.println("\nIndexScan Test: ? = " + i);
    System.out.println("Query: " + INDEXSCAN);
    PreparedStatement stmt = conn.prepareStatement(INDEXSCAN);
    stmt.setInt(1, i);
    ResultSet r = stmt.executeQuery();
    while (r.next()) {
      System.out.println("IndexScanTest got tuple: id: " + r.getString(1) + ", data: " + r.getString(2));
    }
    r.close();
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
    pgstmt.setPrepareThreshold(1);
    stmt.setInt(1, i);
    stmt.setInt(2, i);
    ResultSet r = stmt.executeQuery();

    System.out.println(r.getString(1));
  }


  static public void main(String[] args) throws Exception {
    PelotonTest pt = new PelotonTest();
    pt.Init();
    //pt.Insert(3, TABLE.A);
    pt.Insert(20);
    //pt.ReadModifyWrite(3);
    //pt.BitmapScan(2, 5);
    //pt.SeqScan();
    //pt.DeleteByIndexScan(3);
    //pt.SeqScan();
    //pt.UpdateBySeqScan();
    //pt.IndexScan(3);
    pt.Close();
  }
}
