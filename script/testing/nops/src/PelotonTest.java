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
			"jdbc:postgresql://localhost:54321/postgres", // PELOTON
			"jdbc:postgresql://localhost:5432/postgres", // POSTGRES
			"jdbc:timesten:client:TTC_SERVER_DSN=xxx;TTC_SERVER=xxx;TCP_PORT=xxx" }; // TIMESTEN

	private final String[] user = { "postgres", "postgres", "xxx" };
	private final String[] pass = { "postgres", "postgres", "xxx" };
	private final String[] driver = { "org.postgresql.Driver",
			"org.postgresql.Driver", "com.timesten.jdbc.TimesTenDriver" };

	private final int LOG_LEVEL = 0;

	// Query types
	public static final int SEMICOLON = 0;
	public static final int SIMPLE_SELECT = 1;
	public static final int BATCH_UPDATE = 2;
	public static final int COMPLEX_SELECT = 3;
	public static final int SIMPLE_UPDATE = 4;
	public static final int LARGE_UPDATE = 5;

	public static int numThreads = 1;

	// Endpoint types
	public static final int PELOTON = 0;
	public static final int POSTGRES = 1;
	public static final int TIMESTEN = 2;

	// QUERY TEMPLATES
	private final String DROP = "DROP TABLE IF EXISTS A;";

	private final String DDL = "CREATE TABLE A (id INT PRIMARY KEY, data VARCHAR(100), "
			+ "field1 VARCHAR(100), field2 VARCHAR(100), field3 VARCHAR(100), field4 VARCHAR(100), "
			+ "field5 VARCHAR(100), field6 VARCHAR(100), field7 VARCHAR(100), field8 VARCHAR(100), field9 VARCHAR(100));";

	private final String INDEXSCAN_PARAM = "SELECT * FROM A WHERE id = ?";

	private final String UPDATE_BY_INDEXSCAN = "UPDATE A SET id=99 WHERE id=?";

	private final String UPDATE_BY_LARGE_DATA = "UPDATE A SET data = ?, "
			+ "field1 = ?, field2 = ?, field3 = ?, field4 = ?, "
			+ "field5 = ?, field6 = ?, field7 = ?, field8 = ?, field9 = ? WHERE id = 99;";

	private final String LARGE_STRING = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
			+ "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

	private final Connection conn;

	private enum TABLE {
		A, B, AB
	}

	private int target;
	private int query_type;

	class QueryWorker extends Thread {
		public long runningTime;
		public long totalOps = 0;
		public Connection connection;

		public QueryWorker(long runningTime) {
			this.runningTime = runningTime;
			try {
				this.connection = DriverManager.getConnection(url[target],
						user[target], pass[target]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void run() {
			try {
				long startTime = System.currentTimeMillis();
				Statement stmt = connection.createStatement();
				PreparedStatement prepStmt = connection
						.prepareStatement(INDEXSCAN_PARAM);
				if (query_type == BATCH_UPDATE || query_type == SIMPLE_UPDATE) {
					prepStmt = connection.prepareStatement(UPDATE_BY_INDEXSCAN);
				} else if (query_type == LARGE_UPDATE) {
					prepStmt = connection
							.prepareStatement(UPDATE_BY_LARGE_DATA);
				}
				connection.setAutoCommit(false);

				long numOps = 1000;
				// TODO refactor me
				while (true) {
					switch (query_type) {
					case SEMICOLON: {
						PerformNopQuery(stmt, numOps);
						break;
					}
					case SIMPLE_SELECT:
					case SIMPLE_UPDATE: {
						for (long i = 0; i < numOps; i++) {
							prepStmt.setInt(1, 0);
							prepStmt.execute();
						}
						break;
					}
					case BATCH_UPDATE: {
						for (long batch = 0; batch < numOps / 10; batch++) {
							for (int i = 1; i <= 10; i++) {
								prepStmt.setInt(1, i);
								prepStmt.addBatch();
							}
							int[] res = prepStmt.executeBatch();
							for (int i = 0; i < res.length; i++) {
								if (res[i] < 0) {
									throw new SQLException("Query " + (i + 1)
											+ " returned " + res[i]);
								}
							}
							connection.commit();
						}
						break;
					}
					case LARGE_UPDATE: {
						for (long i = 0; i < numOps; i++) {
							// We have to set 10 params in total
							for (int j = 1; j <= 10; j++) {
								prepStmt.setString(j, LARGE_STRING);
							}
							prepStmt.execute();
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
		Connection conn = DriverManager.getConnection(url[target],
				user[target], pass[target]);
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
		} else {
			// Well, Timesten doesn't support `if exists` keyword
			try {
				stmt.execute("DROP TABLE A;");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		stmt.execute(DDL);
	}

	public void Timed_Nop_Test() throws Exception {
		int runningTime = 15;
		QueryWorker[] workers = new QueryWorker[numThreads];
		// Initialize all worker threads
		for (int i = 0; i < numThreads; i++) {
			workers[i] = new QueryWorker(1000 * runningTime);
		}
		// Submit to thread pool
		ExecutorService executorPool = Executors.newFixedThreadPool(numThreads);
		for (int i = 0; i < numThreads; i++) {
			executorPool.submit(workers[i]);
		}
		// No more task to submit
		executorPool.shutdown();
		// Wait for tasks to terminate
		executorPool.awaitTermination(runningTime + 3, TimeUnit.SECONDS);
		// Calculate the total number of ops
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
			} catch (Exception e) {
			}
		}
	}

	public void Nop_Test() throws Exception {
		long startTime = System.currentTimeMillis();
		long elapsedTime = 0L;
		long numOps = 200 * 1000;
		// TODO refactor me
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
				} catch (Exception e) {
				}
			}
		} else if (query_type == BATCH_UPDATE) {
			PreparedStatement stmt = conn.prepareStatement(UPDATE_BY_INDEXSCAN);
			conn.setAutoCommit(false);
			for (long batch = 0; batch < numOps / 10; batch++) {
				for (int i = 1; i <= 10; i++) {
					stmt.setInt(1, i);
					stmt.addBatch();
				}
				int[] res = stmt.executeBatch();
				for (int i = 0; i < res.length; i++) {
					if (res[i] < 0) {
						throw new SQLException("Query " + (i + 1)
								+ " returned " + res[i]);
					}
				}
				conn.commit();
			}
		}
		elapsedTime = (new Date()).getTime() - startTime;
		System.out.println("Nop throughput: " + numOps * 1000 / elapsedTime);
	}

	static private void printHelpMessage() {
		System.out
				.println("Please specify target: [peloton|timesten|postgres] "
						+ "[semicolon|select|batch_update|simple_update|large_update]");
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
			System.out
					.println("Please set env var LD_LIBRARY_PATH "
							+ "if you're using timesten. e.g. /home/rxian/TimesTen/ttdev2/lib");
			break;
		}
		default: {
			printHelpMessage();
			return;
		}
		}

		int query_type = SEMICOLON;
		switch (args[1]) {
		case "semicolon": {
			query_type = SEMICOLON;
			break;
		}
		case "select": {
			query_type = SIMPLE_SELECT;
			break;
		}
		case "batch_update": {
			query_type = BATCH_UPDATE;
			break;
		}
		case "simple_update": {
			query_type = SIMPLE_UPDATE;
			break;
		}
		case "large_update": {
			query_type = LARGE_UPDATE;
			break;
		}
		default: {
			printHelpMessage();
			return;
		}
		}

		PelotonTest pt = new PelotonTest(target, query_type);
		pt.Init();

		// If the number of threads is specified, we turn on timed test
		if (args.length == 3) {
			numThreads = Integer.parseInt(args[2]);
			pt.Close();
			pt.Timed_Nop_Test();
		} else {
			pt.Nop_Test();
		}
	}

}
