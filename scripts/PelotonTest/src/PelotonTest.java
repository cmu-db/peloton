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

  private final String DROP = "DROP TABLE IF EXISTS peloton_test";
  private final String DDL = "CREATE TABLE peloton_test (id INT PRIMARY KEY, data TEXT)";
  private final String INSERT = "INSERT INTO peloton_test VALUES (?,?)";
  private final String SEQSCAN = "SELECT * FROM peloton_test WHERE id != ?";
  private final String INDEXSCAN = "SELECT * FROM peloton_test WHERE id = ?";

  private final Connection conn;

  public PelotonTest() throws SQLException {
    try {
      Class.forName("org.postgresql.Driver");
    } catch(ClassNotFoundException e) {
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
   * @throws SQLException
   */
  public void Init() throws SQLException {
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement();
    stmt.execute(DROP);
    stmt.execute(DDL);
    conn.setAutoCommit(false);
    System.out.println("Test db created.");
  }

  /**
   * Insert a record
   * @param i the id of the record
   * @throws SQLException
   */
  public void Insert(int i) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(INSERT);
    String data = "Ming says hello world and id = " + i;
    stmt.setInt(1, i);
    stmt.setString(2, data);
    stmt.executeUpdate();
    System.out.println("Inserted: id: " + i + ", data: " + data);
    return;

  }

  public void SeqScan() throws SQLException {
    System.out.println("SeqScan Test:");
    System.out.println("Query: " + SEQSCAN);
    PreparedStatement stmt = conn.prepareStatement(SEQSCAN);
    stmt.setInt(1, 3);
    ResultSet r = stmt.executeQuery();
    while (r.next()) {
      System.out.println("SeqScan: " + r.getString(2));
    }
  }

  /**
   * Perform Index Scan with a simple equal qualifier
   * @param i the param for the equal qualifier
   * @throws SQLException
   */
  public void IndexScan(int i) throws SQLException {
    System.out.println("IndexScan Test: ? = " + i);
    System.out.println("Query: " + INDEXSCAN);
    PreparedStatement stmt = conn.prepareStatement(INDEXSCAN);
    stmt.setInt(1, i);
    ResultSet r = stmt.executeQuery();
    while (r.next()) {
      System.out.println("IndexScanText got tuple: id: " + r.getString(1) + ", data: " + r.getString(2));
    }
  }


  static public void main(String[] args) throws Exception {
    PelotonTest pt = new PelotonTest();
    pt.Init();
    pt.Insert(1);
    pt.Insert(2);
    pt.Insert(3);
    pt.Insert(4);
    pt.IndexScan(3);
    pt.Close();
  }
}
