import java.sql.PreparedStatement;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StocklevelTest {
	
  public static void main(String[] argv) {

    System.out.println("-------- TPCC Stocklevel Testing ------------");
        
    PreparedStatement pst = null;
    ResultSet rs = null;

    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
      e.printStackTrace();
      return;
    }

    System.out.println("PostgreSQL JDBC Driver Registered!");
    Connection con = null;

    try {
      con = DriverManager.getConnection(
	"jdbc:postgresql://127.0.0.1:15721/tpcc", "tpcc",
	"tpcc");

    String query = "SELECT * " +
		   " FROM order_line " +
		   " WHERE OL_W_ID = ?" +
		   " AND OL_D_ID = ?" +
		   " AND OL_O_ID < ?" +
		   " AND OL_O_ID >= ?"
;

    String query2 = "SELECT * " +
                    " FROM stock " +
                    " WHERE S_W_ID = ?" +
                    " AND S_I_ID < ?" +
                    " AND S_QUANTITY < ?"
;

    String query3 = "SELECT S_I_ID" +
                    " FROM order_line, stock " +
                    " WHERE OL_W_ID = ?" +
                    " AND OL_D_ID = ?" +
                    " AND OL_O_ID < ?" +
                    " AND OL_O_ID >= ?" +
                    " AND S_W_ID = ?" +
                    " AND S_I_ID = OL_I_ID" + 
                    " AND S_QUANTITY < ?"
;
    pst = con.prepareStatement(query3);

    pst.setInt(1, 1);   // OL_W_ID
    pst.setInt(2, 1);   // OL_D_ID
    pst.setInt(3, 2);   // OL_O_ID
    pst.setInt(4, 1);   // OL_O_ID
    pst.setInt(5, 1);   // S_W_ID
    pst.setInt(6, 100);
    boolean isResult = pst.execute();

    do {
      rs = pst.getResultSet();     
      while (rs.next()) {
        System.out.print("Result:");
        System.out.println(rs.getInt(1));
    }
      isResult = pst.getMoreResults();
    }while (isResult);

  } catch (SQLException e) {
    System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
    return;
  }

  if (con != null) {
    System.out.println("You made it, take control your database now!");
  } else {
    System.out.println("Failed to make connection!");
  }
}
}
