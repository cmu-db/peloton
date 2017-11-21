import java.sql.*;
import java.util.Properties;

public class SSLTest {
    private final String url = "jdbc:postgresql://localhost:15721/";
    private final Properties props = new Properties();

    private final String DROP = "DROP TABLE IF EXISTS A;";
    private final String DDL = "CREATE TABLE A (id INT PRIMARY KEY, city TEXT);";
    private final String INSERT_1 = "INSERT INTO A VALUES (1, 'New York')";
    private final String INSERT_2 = "INSERT INTO A VALUES (1, 'Pittsburgh')";
    private final String SELECT_1 = "SELECT * FROM A";

    private final Connection conn;

    public SSLTest() throws SQLException {
        try{
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        props.setProperty("user", "progres");
        props.setProperty("password", "postgres");
        props.setProperty("ssl", "true");
        props.setProperty("sslkey", "../data/client.key");
        props.setProperty("sslrootcert", "../data/root.crt");
        props.setProperty("sslcert", "../data/client.csr");
        conn = makeConnection();
    }

    private Connection makeConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(url, props);
        return conn;
    }

    public void Close() throws SQLException {
        System.out.println("Close called");
        conn.close();
    }

    public void Init() throws SQLException {
        System.out.println("Init");
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        stmt.execute(DROP);
        stmt.execute(DDL);
    }

    public void Basic() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(INSERT_1);
        stmt.execute(INSERT_2);
        ResultSet rs = stmt.getResultSet();
        while (rs.next()) {
            String city_name = rs.getString(0);
            System.out.println(city_name);
        }
        stmt.execute(SELECT_1);
    }

    public static void main(String[] args) throws SQLException {
        SSLTest ssltest = new SSLTest();
        ssltest.Init();
        ssltest.Basic();
        ssltest.Close();
    }
}
