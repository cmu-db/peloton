//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// psql_temp_table_test.cpp
//
// Identification: test/catalog/psql_temp_table_test.cpp
//
// Copyright (c) 2016-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"
#include "common/logger.h"
#include "network/peloton_server.h"
#include "network/protocol_handler_factory.h"
#include "util/string_util.h"
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "network/postgres_protocol_handler.h"
#include "network/connection_handle_factory.h"
#include "parser/postgresparser.h"
#include "concurrency/transaction_manager_factory.h"
#include "catalog/catalog.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Psql temporary table tests
//===--------------------------------------------------------------------===//

class PsqlTempTableTests : public PelotonTest {};

/**
 * Table visibility test - check:
 *   1. a temp table makes the permanent table with the same name invisible
 *   2. a temp table created by one session is invisible to another session
 */
void TableVisibilityTest(int port) {
  LOG_INFO("Start TableVisibilityTest");

  try {
    pqxx::connection C1(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database sslmode=disable "
        "application_name=psql",
        port));

    // Session 1 creates a permanent table, which has one tuple
    LOG_INFO("Session 1 creates a permanent table");
    pqxx::work txn11(C1);
    txn11.exec("DROP TABLE IF EXISTS employee;");
    txn11.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    txn11.exec("INSERT INTO employee VALUES(1, 'trump');");

    // Session 1 creates a temp table with the same name, which has 2 tuples
    LOG_INFO("Session 1 creates a temp table");
    txn11.exec("CREATE TEMP TABLE employee(id INT, name VARCHAR(100));");
    txn11.exec("INSERT INTO employee VALUES(1, 'trump');");
    txn11.exec("INSERT INTO employee VALUES(2, 'trumpet');");
    txn11.commit();

    // Temp table makes the permanent table invisible
    LOG_INFO("Check: Temp table makes the permanent table invisible");
    pqxx::work txn12(C1);
    pqxx::result R = txn12.exec("select * from employee;");
    EXPECT_EQ(R.size(), 2);
    // However the permanent table is still visible if we explicitly specify
    // the "public" namespace
    LOG_INFO("Check: Permanent table is still visible given the namespace");
    R = txn12.exec("select * from public.employee;");
    EXPECT_EQ(R.size(), 1);
    txn12.commit();

    // Set up session 2
    // The table visible to session 2 should be the permanent table
    LOG_INFO("Check: Permanent table is visible to session 2");
    pqxx::connection C2(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database sslmode=disable "
        "application_name=psql",
        port));
    pqxx::work txn21(C2);
    R = txn21.exec("select * from employee;");
    EXPECT_EQ(R.size(), 1);
    txn21.commit();

    // Session 2 drops the permanent table
    LOG_INFO("Session 2 drops the permanent table");
    pqxx::work txn22(C2);
    txn22.exec("drop table employee;");
    txn22.commit();

    // Now no table is visible to session 2. (The temp table created by
    // session 1 is invisible.)
    // Expect an exception: "Table [public.]employee is not found"
    LOG_INFO("Check: No table is visible to session 2");
    pqxx::work txn23(C2);
    try {
      txn23.exec("select * from employee;");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(), "employee is not found") != nullptr);
    }
    C2.disconnect();

    // Session 1 can still see its temp table
    LOG_INFO("Check: Session 1 can still see its temp table");
    pqxx::work txn13(C1);
    R = txn13.exec("select * from employee;");
    EXPECT_EQ(R.size(), 2);
    txn13.commit();

    // Session 1 drops its temp table
    LOG_INFO("Session 1 drops its temp table");
    pqxx::work txn14(C1);
    txn14.exec("drop table employee;");
    txn14.commit();

    // Now no table is visible to session 1
    // Expect an exception: "Table [public.]employee is not found"
    LOG_INFO("Check: No table is visible to session 1");
    pqxx::work txn15(C1);
    try {
      txn15.exec("select * from employee;");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(), "employee is not found") != nullptr);
    }
    C1.disconnect();
    LOG_INFO("Passed TableVisibilityTest");
  } catch (const std::exception &e) {
    LOG_ERROR("[TableVisibilityTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }
}

/**
 * Foreign key test - check foreign key constraints cannot be defined between
 * temporary tables and permanent tables.
 */
void ForeignKeyTest(int port) {
  LOG_INFO("Start ForeignKeyTest");

  try {
    // Set up connection
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database sslmode=disable "
        "application_name=psql",
        port));

    pqxx::work txn1(C);
    // Create permanent table "student"
    LOG_INFO("Create permanent table \"student\"");
    txn1.exec("DROP TABLE IF EXISTS student;");
    txn1.exec("CREATE TABLE student(id INT PRIMARY KEY, name VARCHAR);");
    // Create temp table "course"
    LOG_INFO("Create temp table \"course\"");
    txn1.exec("DROP TABLE IF EXISTS course;");
    txn1.exec("CREATE TEMP TABLE course(id INT PRIMARY KEY, name VARCHAR);");
    txn1.commit();

    // Check a permanent table cannot reference a temp table
    LOG_INFO("Check: A permanent table cannot reference a temp table");
    pqxx::work txn2(C);
    try {
      txn2.exec(
          "CREATE TABLE enroll(s_id INT, c_id INT, "
          "CONSTRAINT FK_EnrollCourse FOREIGN KEY (c_id) "
          "REFERENCES course(id)"  // "course" is a temp table
          ");");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(),
                         "constraints on permanent tables may reference only "
                         "permanent tables") != nullptr);
    }

    pqxx::work txn3(C);
    try {
      txn3.exec(
          "CREATE TABLE enroll2(s_id INT, c_id INT, "
          "CONSTRAINT FK_StudentEnroll FOREIGN KEY (s_id) "
          "REFERENCES student(id), "
          "CONSTRAINT FK_EnrollCourse FOREIGN KEY (c_id) "
          "REFERENCES course(id)"  // "course" is a temp table
          ");");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(),
                         "constraints on permanent tables may reference only "
                         "permanent tables") != nullptr);
    }

    // Check a temp table cannot reference a permanent table
    LOG_INFO("Check: A temp table cannot reference a permanent table");
    pqxx::work txn4(C);
    try {
      txn4.exec(
          "CREATE TEMP TABLE enroll3(s_id INT, c_id INT, "
          "CONSTRAINT FK_StudentEnroll FOREIGN KEY (s_id) "
          "REFERENCES student(id)"  // "student" is a permanent table
          ");");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(),
                         "constraints on temporary tables may reference only "
                         "temporary tables") != nullptr);
    }

    pqxx::work txn5(C);
    try {
      txn5.exec(
          "CREATE TEMP TABLE enroll4(s_id INT, c_id INT, "
          "CONSTRAINT FK_StudentEnroll FOREIGN KEY (s_id) "
          "REFERENCES student(id), "  // "student" is a permanent table
          "CONSTRAINT FK_EnrollCourse FOREIGN KEY (c_id) "
          "REFERENCES course(id)"
          ");");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(),
                         "constraints on temporary tables may reference only "
                         "temporary tables") != nullptr);
    }

    pqxx::work txn6(C);
    // Create temp table "student"
    LOG_INFO("Create temp table \"student\"");
    txn6.exec("CREATE TEMP TABLE student(id INT PRIMARY KEY, name VARCHAR);");
    // Now the permanent table "student" becomes invisible. However, it can
    // still
    // be referenced as a foreign key by a permanent table by explicitly
    // specifying the "public" namespace
    LOG_INFO(
        "Check: A hidden permanent table can be referenced by a given the "
        "\"public\" namespace");
    txn6.exec(
        "CREATE TABLE enroll5(s_id INT, c_id INT, "
        "CONSTRAINT FK_StudentEnroll FOREIGN KEY (s_id) "
        "REFERENCES public.student(id)"  // note the "public." here
        ");");
    txn6.commit();

    C.disconnect();
    LOG_INFO("Passed ForeignKeyTest");
  } catch (const std::exception &e) {
    LOG_ERROR("[ForeignKeyTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }
}

/**
 * On-commit options test - check the options [ON COMMIT PRESERVE ROWS |
 * DELETE ROWS | DROP] are handled correctly
 */
void OnCommitOptionsTest(int port) {
  LOG_INFO("Start OnCommitOptionsTest");

  try {
    pqxx::connection C1(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database sslmode=disable "
        "application_name=psql",
        port));
    pqxx::work txn11(C1);

    // Begin a transaction
    txn11.exec("BEGIN;");

    LOG_INFO("Creating temp table with default on-commit option");
    // The default option is ON COMMIT PRESERVE ROWS
    txn11.exec("DROP TABLE IF EXISTS employee1;");
    txn11.exec("CREATE TEMP TABLE employee1(id INT, name VARCHAR(100));");
    txn11.exec("INSERT INTO employee1 VALUES(1, 'trump');");

    LOG_INFO("Creating temp table with \"ON COMMIT PRESERVE ROWS\"");
    txn11.exec("DROP TABLE IF EXISTS employee2;");
    txn11.exec(
        "CREATE TEMP TABLE employee2(id INT, name VARCHAR(100)) ON COMMIT "
        "PRESERVE ROWS;");
    txn11.exec("INSERT INTO employee2 VALUES(1, 'trump');");
    txn11.exec("INSERT INTO employee2 VALUES(2, 'trumpet');");

    LOG_INFO("Creating temp table with \"ON COMMIT DELETE ROWS\"");
    txn11.exec("DROP TABLE IF EXISTS employee3;");
    txn11.exec(
        "CREATE TEMP TABLE employee3(id INT, name VARCHAR(100)) ON COMMIT "
        "DELETE ROWS;");
    txn11.exec("INSERT INTO employee3 VALUES(1, 'trump');");
    txn11.exec("INSERT INTO employee3 VALUES(2, 'trumpet');");
    txn11.exec("INSERT INTO employee3 VALUES(3, 'trumpette');");

    LOG_INFO("Creating temp table with \"ON COMMIT DROP\"");
    txn11.exec("DROP TABLE IF EXISTS employee4;");
    txn11.exec(
        "CREATE TEMP TABLE employee4(id INT, name VARCHAR(100)) ON COMMIT "
        "DROP;");

    LOG_INFO("Check: all four tables have been created successfully");
    pqxx::result R = txn11.exec("select * from employee1;");
    EXPECT_EQ(R.size(), 1);

    R = txn11.exec("select * from employee2;");
    EXPECT_EQ(R.size(), 2);

    R = txn11.exec("select * from employee3;");
    EXPECT_EQ(R.size(), 3);

    R = txn11.exec("select * from employee4;");
    EXPECT_EQ(R.size(), 0);

    // Commit the transaction
    txn11.exec("COMMIT;");

    LOG_INFO(
        "Check: all rows are preserved for the table created with default "
        "on-commit option");
    R = txn11.exec("select * from employee1;");
    EXPECT_EQ(R.size(), 1);

    LOG_INFO(
        "Check: all rows are preserved for the table created with \"ON COMMIT "
        "PRESERVE ROWS\"");
    R = txn11.exec("select * from employee2;");
    EXPECT_EQ(R.size(), 2);

    // Currently ON COMMIT DELETE ROWS is not supported.
    // LOG_INFO(
    //     "Check: all rows are deleted for the table created with \"ON COMMIT "
    //     "DELETE ROWS\"");
    // R = txn11.exec("select * from employee3;");
    // EXPECT_EQ(R.size(), 0);
    txn11.commit();

    LOG_INFO("Check: the table created with \"ON COMMIT DROP\" is dropped");
    pqxx::work txn12(C1);
    try {
      txn12.exec("select * from employee4;");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(), "employee4 is not found") != nullptr);
    }

    C1.disconnect();

    pqxx::connection C2(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database sslmode=disable "
        "application_name=psql",
        port));

    LOG_INFO("Check: all tables are dropped when the session is closed");
    pqxx::work txn21(C2);
    try {
      txn21.exec("select * from employee1;");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(), "employee1 is not found") != nullptr);
    }

    pqxx::work txn22(C2);
    try {
      txn22.exec("select * from employee2;");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(), "employee2 is not found") != nullptr);
    }

    pqxx::work txn23(C2);
    try {
      txn23.exec("select * from employee3;");
      EXPECT_TRUE(false);
    } catch (const std::exception &e) {
      EXPECT_TRUE(strstr(e.what(), "employee3 is not found") != nullptr);
    }

    C2.disconnect();
    LOG_INFO("Passed OnCommitOptionsTest");
  } catch (const std::exception &e) {
    LOG_ERROR("[OnCommitOptionsTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }
}

TEST_F(PsqlTempTableTests, PsqlTempTableTests) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");

  peloton::network::PelotonServer server;

  int port = 15721;
  try {
    server.SetPort(port);
    server.SetupServer();
  } catch (peloton::ConnectionException &exception) {
    LOG_INFO("[LaunchServer] Exception when launching server");
  }
  std::thread serverThread([&]() { server.ServerLoop(); });

  TableVisibilityTest(port);
  ForeignKeyTest(port);
  OnCommitOptionsTest(port);

  server.Close();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton