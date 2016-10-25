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
import java.util.concurrent.*;

public class PelotonTest {

  // Peloton, Postgres, Timesten Endpoints
  private final String[] url = {
     "jdbc:postgresql://localhost:54321/postgres",  // PELOTON 
     "jdbc:postgresql://localhost:5432/postgres",   // POSTGRES
     "jdbc:timesten:client:TTC_SERVER_DSN=xxx;TTC_SERVER=xxx;TCP_PORT=xxx"}; // TIMESTEN

  private final String[] user = {"postgres", "postgres", "xxx"};
  private final String[] pass = {"postgres", "postgres", "xxx"};
  private final String[] driver = {"org.postgresql.Driver", "org.postgresql.Driver", "com.timesten.jdbc.TimesTenDriver"};
  
  private final int LOG_LEVEL = 0;
  
  // Query types
  public static final int SEMICOLON = 0;
  public static final int SIMPLE_SELECT = 1;
  public static final int BATCH_UPDATE = 2;
  public static final int COMPLEX_SELECT = 3;
  
  public static int numThreads = 1;

  // Endpoint types
  public static final int PELOTON = 0;
  public static final int POSTGRES = 1;
  public static final int TIMESTEN = 2;
    
  // QUERY TEMPLATES
  private final String DROP = "DROP TABLE IF EXISTS A;" +
          "DROP TABLE IF EXISTS B;";
  private final String DDL = "CREATE TABLE A (id INT PRIMARY KEY, data TEXT, field1 INT, field2 INT, field3 INT, field4 INT);";

  private final String INDEXSCAN_PARAM = "SELECT * FROM A WHERE id = ?";

  private final String INDEXSCAN_PARAM_MULTI_VAR = 
		  "SELECT * FROM A WHERE id = ? AND field1 = ? AND field2 = ? AND field3 = ? AND field4 = ?";
  
  private final String UPDATE_BY_INDEXSCAN = "UPDATE A SET id=99 WHERE id=?";
  
  private final Connection conn;
  private enum TABLE {A, B, AB}
  
  private int target; 
  private int query_type;

  class QueryWorker extends Thread {
	    public long runningTime;
	    public long totalOps = 0;
	    public Connection connection;
	    
	    public QueryWorker(long runningTime) {
	        this.runningTime = runningTime;
	        try {
	        	this.connection = DriverManager.getConnection(url[target], user[target], pass[target]);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
	    }
	    
	    public void run() {
	        try {
		        long startTime = System.currentTimeMillis();
	            Statement stmt = connection.createStatement();
	            PreparedStatement prepStmt = connection.prepareStatement(INDEXSCAN_PARAM);
	            if (query_type == COMPLEX_SELECT) {
	            	prepStmt = connection.prepareStatement(INDEXSCAN_PARAM_MULTI_VAR);
	            } else if (query_type == BATCH_UPDATE) {
	            	prepStmt = connection.prepareStatement(UPDATE_BY_INDEXSCAN);	
	            }
	            connection.setAutoCommit(false);

		        long numOps = 1000;
		        while (true) {
		        	switch (query_type) {
			        	case SEMICOLON: {
				        	PerformNopQuery(stmt, numOps);
				        	break;
			        	}
			        	case SIMPLE_SELECT: {
				            for (long i = 0; i < numOps; i++) {
				               try {
				            	   prepStmt.setInt(1, 0);
				            	   prepStmt.execute();
				            	   connection.commit();
				               } catch (Exception e) {
				            	   e.printStackTrace();
				               }
				            }
				            break;
			        	}
			        	case COMPLEX_SELECT: {
				            for (long i = 0; i < numOps; i++) {
					            try {
					            	prepStmt.setInt(1, 0);
					            	prepStmt.setInt(2, 0);
					            	prepStmt.setInt(3, 0);
					            	prepStmt.setInt(4, 0);
					            	prepStmt.setInt(5, 0);			            	
					            	prepStmt.execute();
					            	connection.commit();
					            } catch (Exception e) {
					            	e.printStackTrace();
					            }
					        }
					        break;
			        	}
			        	case BATCH_UPDATE: {
			                for (long batch = 0; batch < numOps / 10; batch++) { 
			          	      for(int i=1; i <= 10;i++){
			          	    	prepStmt.setInt(1,i);
			          	        prepStmt.addBatch();
			          	      }
			          	      int[] res = prepStmt.executeBatch();
			          	      for(int i=0; i < res.length; i++) {
			          		      if (res[i] < 0) {
			          		          throw new SQLException("Query "+ (i+1) +" returned " + res[i]);
			          		      }
			          	      }
			          	      connection.commit();
			                }
			                break;
			        	}
			        	default: {
			        		System.out.println("Unrecognized query type");
			        	}
		        	}		        	
		        	
		        	totalOps += numOps;
		            if (runningTime < (System.currentTimeMillis() - startTime)) {
		                break;
		            }
		        }
		        connection.close();
	        } catch (Exception e) { 
	        	e.printStackTrace();
	        }
	    }
	}

  
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

  
  public void Timed_Nop_Test() throws Exception {
	  int runningTime = 15;
	  QueryWorker[] workers = new QueryWorker[numThreads];
	  for (int i = 0; i < numThreads; i++) {
		  workers[i] = new QueryWorker(1000 * runningTime);
	  }
	  ExecutorService executorPool = Executors.newFixedThreadPool(numThreads);
      for (int i = 0; i < numThreads; i++) {
    	  // TODO use invokeAll
    	  executorPool.submit(workers[i]);	  
      }
      executorPool.shutdown();
      executorPool.awaitTermination(runningTime + 3, TimeUnit.SECONDS);
      long totalThroughput = 0;
      for (int i = 0; i < numThreads; i++) {
    	  totalThroughput += workers[i].totalOps * 1.0 / runningTime;
      }
	  System.out.println(totalThroughput);
  }
  
  private void PerformNopQuery(Statement stmt, long numOps) throws Exception {
      for (long i = 0; i < numOps; i++) {
        try {
            stmt.execute(";");
        } catch (Exception e) { }
      } 
  }
  
  public void Nop_Test() throws Exception {
    long startTime = System.currentTimeMillis();
    long elapsedTime = 0L;
    long numOps = 200 * 1000;
    if (query_type == SEMICOLON) {
    	Statement stmt = conn.createStatement();
    	PerformNopQuery(stmt, numOps);
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
      for (long batch = 0; batch < numOps / 10; batch++) { 
	      for(int i=1; i <= 10;i++){
	        stmt.setInt(1,i);
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
      switch (args[0]) {	      
	      case ("peloton"): {
	        target = PELOTON;
	        break;
	      } 
	      case ("postgres"): {
	        target = POSTGRES;
	        break;
	      } 
	      case ("timesten"): {
	        target = TIMESTEN;
	        System.out.println("Please set env var LD_LIBRARY_PATH " + 
	  	          "if you're using timesten. e.g. /home/rxian/TimesTen/ttdev2/lib");
	        break;
	      } 
	      default: {
	    	printHelpMessage();
	      	return;
	      }
      }

      int query_type = SEMICOLON;
      switch (args[1]) {
	      case "semicolon":  {
	        query_type = SEMICOLON;
	        break;
	      }
	      case "select": {
	    	query_type = SIMPLE_SELECT;
	    	break;
	      } 
	      case "complex_select": {
	    	query_type = COMPLEX_SELECT;
	    	break;
	      }
	      case "batch": {
	    	query_type = BATCH_UPDATE;
	    	break;
	      } 
	      default: {
	      	printHelpMessage();
	      	return;
	      }
      }
      
      PelotonTest pt = new PelotonTest(target, query_type);      
      pt.Init();          
      
      if (args.length == 3) {
    	  numThreads = Integer.parseInt(args[2]);
          pt.Close();
          pt.Timed_Nop_Test();    	  
      } else {
          pt.Nop_Test();
      }
  }

}
