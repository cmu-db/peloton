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
void *TableVisibilityTest(int port) {
  pqxx::connection C1(StringUtil::Format(
      "host=127.0.0.1 port=%d user=default_database sslmode=disable "
      "application_name=psql",
      port));

  // Connection 1 creates a permanent table, which has one tuple
  pqxx::work txn11(C1);
  txn11.exec("DROP TABLE IF EXISTS employee;");
  txn11.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
  txn11.exec("INSERT INTO employee VALUES(1, 'trump');");

  // Connection 1 creates a temp table with the same name, which has 2 tuples
  txn11.exec("CREATE TEMP TABLE employee(id INT, name VARCHAR(100));");
  txn11.exec("INSERT INTO employee VALUES(1, 'trump');");
  txn11.exec("INSERT INTO employee VALUES(2, 'trumpet');");
  txn11.commit();

  // Temp table makes the permanent table invisible
  pqxx::work txn12(C1);
  pqxx::result R = txn12.exec("select * from employee;");
  EXPECT_EQ(R.size(), 2);
  // However the permanent table is still visible if we explicitly specify
  // the "public" namespace
  R = txn12.exec("select * from public.employee;");
  EXPECT_EQ(R.size(), 1);
  txn12.commit();

  // Set up connection 2
  // The table visible to connection 2 should be the permanent table
  pqxx::connection C2(StringUtil::Format(
      "host=127.0.0.1 port=%d user=default_database sslmode=disable "
      "application_name=psql",
      port));
  pqxx::work txn21(C2);
  R = txn21.exec("select * from employee;");
  EXPECT_EQ(R.size(), 1);
  txn21.commit();

  // Connection 2 drops the permanent table
  pqxx::work txn22(C2);
  txn22.exec("drop table employee;");
  txn22.commit();

  // Now no table is visible to connection 2. (The temp table created by
  // connection 1 is invisible.)
  // Expect an exception: "Table employee is not found"
  pqxx::work txn23(C2);
  try {
    txn23.exec("select * from employee;");
    EXPECT_TRUE(false);
  } catch (const std::exception &e) {
    LOG_INFO("Exception occurred (as expected): %s", e.what());
  }
  C2.disconnect();

  // Connection 1 can still see its temp table
  pqxx::work txn13(C1);
  R = txn13.exec("select * from employee;");
  EXPECT_EQ(R.size(), 2);
  txn13.commit();

  // Connection 1 drops its temp table
  pqxx::work txn14(C1);
  txn14.exec("drop table employee;");
  txn14.commit();

  // Now no table is visible to connection 1
  // Expect an exception: "Table employee is not found"
  pqxx::work txn15(C1);
  try {
    txn15.exec("select * from employee;");
    EXPECT_TRUE(false);
  } catch (const std::exception &e) {
    LOG_INFO("Exception occurred (as expected): %s", e.what());
  }
  C1.disconnect();

  LOG_INFO("Passed TableVisibilityTest");
  return NULL;
}

/**
 * Foreign key test - check foreign key constraints cannot be defined between
 * temporary tables and permanent tables.
 */
void *ForeignKeyTest(int port) {
  // Set up connection
  pqxx::connection C(StringUtil::Format(
      "host=127.0.0.1 port=%d user=default_database sslmode=disable "
      "application_name=psql",
      port));

  pqxx::work txn1(C);
  // Create permanent table "student"
  txn1.exec("DROP TABLE IF EXISTS student;");
  txn1.exec("CREATE TABLE student(id INT PRIMARY KEY, name VARCHAR);");
  // Create temp table "course"
  txn1.exec("DROP TABLE IF EXISTS course;");
  txn1.exec("CREATE TEMP TABLE course(id INT PRIMARY KEY, name VARCHAR);");
  txn1.commit();

  // Check a permanent table cannot reference a temp table directly
  pqxx::work txn2(C);
  EXPECT_THROW(txn2.exec(
                   "CREATE TABLE enroll(s_id INT, c_id INT, "
                   "CONSTRAINT FK_EnrollCourse FOREIGN KEY (c_id) "
                   "REFERENCES course(id)"  // "course" is a temp table
                   ");"),
               ConstraintException);
  txn2.commit();

  pqxx::work txn3(C);
  EXPECT_THROW(txn3.exec(
                   "CREATE TABLE enroll2(s_id INT, c_id INT, "
                   "CONSTRAINT FK_StudentEnroll FOREIGN KEY (stu_id) "
                   "REFERENCES student(id), "
                   "CONSTRAINT FK_EnrollCourse FOREIGN KEY (c_id) "
                   "REFERENCES course(id)"  // "course" is a temp table
                   ");"),
               ConstraintException);
  txn3.commit();

  // Check a temp table cannot reference a permanent table directly
  pqxx::work txn4(C);
  EXPECT_THROW(txn4.exec(
                   "CREATE TEMP TABLE enroll3(s_id INT, c_id INT, "
                   "CONSTRAINT FK_StudentEnroll FOREIGN KEY (stu_id) "
                   "REFERENCES student(id)"  // "student" is a permanent table
                   ");"),
               ConstraintException);
  txn4.commit();

  pqxx::work txn5(C);
  EXPECT_THROW(txn5.exec(
                   "CREATE TEMP TABLE enroll4(s_id INT, c_id INT, "
                   "CONSTRAINT FK_StudentEnroll FOREIGN KEY (stu_id) "
                   "REFERENCES student(id), "  // "student" is a permanent table
                   "CONSTRAINT FK_EnrollCourse FOREIGN KEY (c_id) "
                   "REFERENCES course(id)"
                   ");"),
               ConstraintException);
  txn5.commit();

  pqxx::work txn6(C);
  // Create temp table "student"
  txn6.exec("CREATE TEMP TABLE student(id INT PRIMARY KEY, name VARCHAR);");
  // Now the permanent table "student" becomes invisible. However, it can still
  // be referenced as a foreign key by a permanent table by explicitly
  // specifying the "public" namespace
  
  txn6.exec(
      "CREATE TABLE enroll5(s_id INT, c_id INT, "
      "CONSTRAINT FK_StudentEnroll FOREIGN KEY (stu_id) "
      "REFERENCES public.student(id)"  // note the "public." here
      ");");
  txn6.commit();

  C.disconnect();
  LOG_INFO("Passed ForeignKeyTest");
  return NULL;
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

  server.Close();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton