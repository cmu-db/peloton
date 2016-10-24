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
  
  // Query types
  public static final int SEMICOLON = 0;
  public static final int SIMPLE_SELECT = 1;
  public static final int BATCH_UPDATE = 2;

  // Endpoint types
  public static final int PELOTON = 0;
  public static final int POSTGRES = 1;
  public static final int TIMESTEN = 2;
  
  // QUERY TEMPLATES
  private final String DROP = "DROP TABLE IF EXISTS A;" +
          "DROP TABLE IF EXISTS B;";
  private final String DDL = "CREATE TABLE A (id INT PRIMARY KEY, data TEXT);";

  private final String INDEXSCAN_PARAM = "SELECT * FROM A WHERE id = ?";

  private final String UPDATE_BY_INDEXSCAN = "UPDATE A SET data=? WHERE id=?";
  
  private final Connection conn;
  private enum TABLE {A, B, AB}
  
  private int target; 
  private int query_type;

  public PelotonTest(int target, int query_type) throws SQLException {
    this.target = target;
    this.query_type = query_type;
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
    } else if (query_type == SIMPLE_SELECT) {
      PreparedStatement stmt = conn.prepareStatement(INDEXSCAN_PARAM);
      conn.setAutoCommit(false);
      for (long i = 0; i < numOps; i++) {
         try {
            stmt.setInt(1, 0);
            stmt.execute();
            conn.commit();
         } catch (Exception e) { }
      }
    } else if (query_type == BATCH_UPDATE) {
      PreparedStatement stmt = conn.prepareStatement(UPDATE_BY_INDEXSCAN);
      conn.setAutoCommit(false);
      numOps /= 10;
      for (long batch = 0; batch < numOps / 10; batch++) { 
	      for(int i=1; i <= 10;i++){
	        stmt.setString(1,"x");
	        stmt.setInt(2,i);
	        stmt.addBatch();
	      }
	      int[] res = stmt.executeBatch();
	      for(int i=0; i < res.length; i++) {
		      if (res[i] < 0) {
		          throw new SQLException("Query "+ (i+1) +" returned " + res[i]);
		      }
	      }
	      conn.commit();
      }
    }
    elapsedTime = (new Date()).getTime() - startTime;
    System.out.println("Nop throughput: " + numOps * 1000 / elapsedTime);
  }

  static private void printHelpMessage() {
	  System.out.println("Please specify target: [peloton|timesten|postgres] [semicolon|select|batch]");
  }
  
  static public void main(String[] args) throws Exception {
	  if (args.length < 2) {
    	printHelpMessage();
    	return;
      }
      int target = PELOTON;
      if (args[0].equals("peloton")) {
        target = PELOTON;
      } else if (args[0].equals("postgres")) {
        target = POSTGRES;
        System.out.println("Please set env var LD_LIBRARY_PATH " + 
          "if you're using timesten. e.g. /home/rxian/TimesTen/ttdev2/lib");
      } else if (args[0].equals("timesten")) {
        target = TIMESTEN;
      } else {
    	printHelpMessage();
      	return;
      }

      // TODO change to switch statement
      int query_type = SEMICOLON;
      if (args[1].equals("semicolon")) {
        query_type = SEMICOLON;
      } else if (args[1].equals("select")) {
    	query_type = SIMPLE_SELECT;
      } else if (args[1].equals("batch")) {
    	query_type = BATCH_UPDATE;
      } else {
    	printHelpMessage();
      	return;
      }
      
      PelotonTest pt = new PelotonTest(target, query_type);
      pt.Init();
      pt.Nop_Test();
      pt.Close();
  }

}
