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
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
import java.lang.reflect.Method;

public class ExchangeTest {
    private final String url = "jdbc:postgresql://localhost:54321/";
    private final String username = "postgres";
    private final String pass = "postgres";

    private final String DROP = "DROP TABLE IF EXISTS A;";
    private final String DDL = "CREATE TABLE A (id INT PRIMARY KEY, name TEXT);";

    public final static String[] nameTokens = { "BAR", "OUGHT", "ABLE", "PRI",
      "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };
    
    private final Random rand;
    
    private final String TEMPLATE_FOR_BATCH_INSERT = "INSERT INTO A VALUES (?,?);";

    private final String SEQSCAN = "SELECT * FROM A";

    private final String NON_KEY_SCAN = "SELECT * FROM A WHERE name = ?";

    private final Connection conn;

    private static final int BATCH_SIZE = 10000;
    
    private static int numRows;

    public ExchangeTest() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        conn = this.makeConnection();
        rand = new Random();
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
    }

    public void SeqScan() throws SQLException {
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        int rowCtr = 0;

        stmt.execute(SEQSCAN);

        ResultSet rs = stmt.getResultSet();

        ResultSetMetaData rsmd = rs.getMetaData();

        if (rsmd.getColumnCount() != 2)
          throw new SQLException("Table should have 2 columns");

        while(rs.next())
            rowCtr++;

        if (rowCtr != numRows)
            throw new SQLException("Insufficient rows returned:" +
                rowCtr +"/" + numRows);
        else
            System.out.println("Sequential Scan successful");
    }

    public void BatchInsert() throws SQLException{
        int[] res;
        int insertCount = 0, numInsertions;
        int numBatches = (numRows + BATCH_SIZE - 1)/BATCH_SIZE;
        String name1, name2;
        PreparedStatement stmt = conn.prepareStatement(TEMPLATE_FOR_BATCH_INSERT);
        conn.setAutoCommit(false);

        for (int i=1; i<=numBatches; i++) {
            numInsertions = (i==numBatches) ? numRows - insertCount : BATCH_SIZE;
            for(int j=1; j <= numInsertions; j++){
                stmt.setInt(1,j+insertCount);
                stmt.setString(2, nameTokens[j%nameTokens.length]);
                stmt.addBatch();
            }
            try{
                res = stmt.executeBatch();
            }catch(SQLException e){
                e.printStackTrace();
                throw e.getNextException();
            }

            for(int k=0; k < res.length; k++){
                if (res[k] < 0) {
                    throw new SQLException("Query "+ (k+1) +" returned " + res[k]);
                }
            }
            insertCount += res.length;
            System.out.println("Inserted " + insertCount + 
                " rows out of " + numRows + " rows.");
            stmt.clearBatch();
        }
        conn.commit();
    }

    public void Selectivity10Scan() throws SQLException {
        int rowCtr = 0;
        conn.setAutoCommit(true);
        PreparedStatement pstmt = conn.prepareStatement(NON_KEY_SCAN);
        pstmt.setString(1, nameTokens[1]);
        pstmt.execute();

        ResultSet rs = pstmt.getResultSet();

        ResultSetMetaData rsmd = rs.getMetaData();

        if (rsmd.getColumnCount() != 2)
          throw new SQLException("Table should have 2 columns");

        while(rs.next())
            rowCtr++;

        if (rowCtr != numRows/10)
            throw new SQLException("Insufficient rows returned:" +
                rowCtr +"/" + numRows/10);
        else
            System.out.println("Selectivity10 Scan successful");
    }

    public void TimeAndExecuteQuery(Object obj, Method method) 
        throws Exception {
            Object[] parameters = new Object[0];
            long startTime, endTime;
            startTime = System.nanoTime();
            method.invoke(obj, parameters);
            endTime = System.nanoTime();
            System.out.println(method.getName() + " runtime:" + 
                ((double) endTime - startTime)/Math.pow(10, 6) + "ms");
    }
  
    static public void main(String[] args) throws Exception {
        boolean isCreate, isExecute, isLoad;
        Options options = new Options();
        long startTime, endTime;
        Class[] parameterTypes = new Class[0];

        // load CLI options
        Option rows = new Option("r", "rows", true,
            "Required: number of input rows");
        rows.setRequired(true);
        Option create = new Option("c", "create", true,
            "Create a new table (true or false)");
        Option load = new Option("l", "load", true,
            "Load values into the table (true or false)");
        Option execute = new Option("e", "execute", true,
            "Execute queries on the table (true or false)");
        options.addOption(rows);
        options.addOption(create);
        options.addOption(load);
        options.addOption(execute);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp(ExchangeTest.class.getName(), options);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("rows")){
            numRows = Integer.parseInt(cmd.getOptionValue("rows"));
        }

        isCreate = Boolean.parseBoolean(cmd.getOptionValue("create", "true"));
        isLoad = Boolean.parseBoolean(cmd.getOptionValue("load", "true"));
        isExecute = Boolean.parseBoolean(cmd.getOptionValue("execute", "true"));

        // select the tests that will be run
        Method test1 = ExchangeTest.class.getMethod("Selectivity10Scan",
                                                    parameterTypes);

        ExchangeTest et = new ExchangeTest();
        if (isCreate) {
            et.Init();
            System.out.println("Completed Init");
        }
        if (isLoad) {
            et.BatchInsert();
            System.out.println("Completed Batch Insert");
        }
        if (isExecute) {
            et.TimeAndExecuteQuery(et, test1);     
        }
        et.Close();
    }
}
