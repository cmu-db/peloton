//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// IndexTest.java
//
// Identification: script/testing/junit/IndexTest.java
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * Index concurrency tests.
 */

import java.sql.*;
import org.junit.*;
import static org.junit.Assert.assertEquals;

public class IndexTest extends PLTestBase {
    private Connection conn;

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl(c1 int, c2 int);";

    /**
     * Initialize the database and table for testing
     */
    private void InitDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
        for (int i = 0; i < 10; i++) {
            stmt.execute("INSERT INTO tbl VALUES (" + i + ", " + i + ");");
        }
    }
    
    @Before
    public void Setup() throws SQLException {
	    conn = makeDefaultConnection();
	    conn.setAutoCommit(true);
	    InitDatabase();
    }

    @After
    public void Teardown() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
    }

    /* --------------------------------------------
     * Index concurrency tests
     * ---------------------------------------------
     */
    
    /**
     * 1 thread create index, 1 thread insert tuples
     */
    @Test
    public void test_create_insert() throws SQLException, InterruptedException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    Thread.sleep(1000);
                    stmt.execute("CREATE INDEX i1 ON tbl(c1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("INSERT INTO tbl VALUES(-1, -1);");
                    Thread.sleep(2000);
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 0;");
        if (rs.next()) {
            checkRow(rs,
                new String [] {"c1", "c2"},
                new int [] {-1, -1});
            assertNoMoreRows(rs);
        } else {
            assert(false);
        }
    }

    /**
     * 1 thread create index concurrently, 1 thread insert tuples
     */
    @Test
    public void test_con_create_insert() throws SQLException, InterruptedException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    Thread.sleep(1000);
                    stmt.execute("CREATE INDEX CONCURRENTLY i1 ON tbl(c1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("INSERT INTO tbl VALUES(-1, -1);");
                    Thread.sleep(2000);
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 0;");
        if (rs.next()) {
            checkRow(rs,
                new String [] {"c1", "c2"},
                new int [] {-1, -1});
            assertNoMoreRows(rs);
        } else {
            assert(false);
        }
    }

    /**
     * 1 thread create index concurrently, 1 thread insert tuples, without sleep
     */
    @Test
    public void test_con_create_insert2() throws SQLException, InterruptedException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("CREATE INDEX CONCURRENTLY i1 ON tbl(c1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("INSERT INTO tbl VALUES(-1, -1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 0;");
        if (rs.next()) {
            checkRow(rs,
                new String [] {"c1", "c2"},
                new int [] {-1, -1});
            assertNoMoreRows(rs);
        } else {
            assert(false);
        }
    }

    /**
     * 1 thread create index, 1 thread update tuples
     */
    @Test
    public void test_create_update() throws SQLException, InterruptedException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    Thread.sleep(1000);
                    stmt.execute("CREATE INDEX i1 ON tbl(c1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("UPDATE tbl SET c1 = -1 WHERE c1 = 0;");
                    Thread.sleep(2000);
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 1;");
        if (rs.next()) {
            checkRow(rs,
                new String [] {"c1", "c2"},
                new int [] {-1, 0});
            assertNoMoreRows(rs);
        } else {
            assert(false);
        }
    }

    /**
     * 1 thread create index concurrently, 1 thread update tuples
     */
    @Test
    public void test_con_create_update() throws SQLException, InterruptedException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    Thread.sleep(1000);
                    stmt.execute("CREATE INDEX CONCURRENTLY i1 ON tbl(c1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("UPDATE tbl SET c1 = -1 WHERE c1 = 0;");
                    Thread.sleep(2000);
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 1;");
        if (rs.next()) {
            checkRow(rs,
                new String [] {"c1", "c2"},
                new int [] {-1, 0});
            assertNoMoreRows(rs);
        } else {
            assert(false);
        }
    }

    /**
     * 1 thread create index concurrently, 1 thread update tuples, without sleep
     * Due to bugs in Bw-Tree index or update_executer, this test fails
     */
//  @Test
    public void test_con_create_update2() throws SQLException, InterruptedException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("CREATE INDEX CONCURRENTLY i1 ON tbl(c1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("UPDATE tbl SET c1 = -1 WHERE c1 = 0;");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 1;");
        if (rs.next()) {
            checkRow(rs,
                new String [] {"c1", "c2"},
                new int [] {-1, 0});
            assertNoMoreRows(rs);
        } else {
            assert(false);
        }
    }

    /**
     * 1 thread create index, 1 thread delete tuples
     */
    @Test
    public void test_create_delete() throws SQLException, InterruptedException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    Thread.sleep(1000);
                    stmt.execute("CREATE INDEX i1 ON tbl(c1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("DELETE FROM tbl WHERE c1 = 0;");
                    Thread.sleep(2000);
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 2;");
        if (rs.next()) {
            checkRow(rs,
                new String [] {"c1", "c2"},
                new int [] {1, 1});
            assertNoMoreRows(rs);
        } else {
            assert(false);
        }
    }

    /**
     * 1 thread create index concurrently, 1 thread delete tuples
     */
    @Test
    public void test_con_create_delete() throws SQLException, InterruptedException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    Thread.sleep(1000);
                    stmt.execute("CREATE INDEX CONCURRENTLY i1 ON tbl(c1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("DELETE FROM tbl WHERE c1 = 0;");
                    Thread.sleep(2000);
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 2;");
        if (rs.next()) {
            checkRow(rs,
                new String [] {"c1", "c2"},
                new int [] {1, 1});
            assertNoMoreRows(rs);
        } else {
            assert(false);
        }
    }

    /**
     * 1 thread create index concurrently, 1 thread delete tuples, without sleep
     */
    @Test
    public void test_con_create_delete2() throws SQLException, InterruptedException {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("CREATE INDEX CONCURRENTLY i1 ON tbl(c1);");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection c = makeDefaultConnection();
                    Statement stmt = c.createStatement();
                    stmt.execute("BEGIN;");
                    stmt.execute("DELETE FROM tbl WHERE c1 = 0;");
                    stmt.execute("END;");
                    stmt.close();
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            }

        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 < 2;");
        if (rs.next()) {
            checkRow(rs,
                new String [] {"c1", "c2"},
                new int [] {1, 1});
            assertNoMoreRows(rs);
        } else {
            assert(false);
        }
    }

}
