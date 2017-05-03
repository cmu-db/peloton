//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// prepare_stmt_test.cpp
//
// Identification: test/wire/prepare_stmt_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"
#include "common/logger.h"
#include "wire/libevent_server.h"
#include "util/string_util.h"
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Prepare Stmt Tests
//===--------------------------------------------------------------------===//

class PrepareStmtTests : public PelotonTest {};

<<<<<<< HEAD
static void *LaunchServer(peloton::wire::LibeventServer libeventserver,
                          int port) {
  try {
    libeventserver.SetPort(port);
=======
static void *LaunchServer(peloton::wire::LibeventServer libeventserver,int port) {
  try {
	libeventserver.SetPort(port);
>>>>>>> 353f1b8... Split packet_manager_test into simple_query_test and prepare_stmt_test
    libeventserver.StartServer();
  } catch (peloton::ConnectionException exception) {
    LOG_INFO("[LaunchServer] exception in thread");
  }
  return NULL;
}

/**
 * named prepare statement without parameters
 * TODO: add prepare's parameters when parser team fix the bug
 */

void *PrepareStatementTest(int port) {
  try {
    pqxx::connection C(StringUtil::Format(
<<<<<<< HEAD
        "host=127.0.0.1 port=%d user=postgres sslmode=disable", port));
    LOG_INFO("[PrepareStatementTest] Connected to %s", C.dbname());
    pqxx::work txn1(C);

=======
            "host=127.0.0.1 port=%d user=postgres sslmode=disable",port));
    LOG_INFO("[PrepareStatementTest] Connected to %s", C.dbname());
    pqxx::work W(C);
    
>>>>>>> 353f1b8... Split packet_manager_test into simple_query_test and prepare_stmt_test
    peloton::wire::LibeventSocket *conn =
        peloton::wire::LibeventServer::GetConn(
            peloton::wire::LibeventServer::recent_connfd);

    // create table and insert some data
<<<<<<< HEAD
    txn1.exec("DROP TABLE IF EXISTS employee;");
    txn1.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    txn1.commit();

    pqxx::work txn2(C);
    txn2.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn2.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn2.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    // test prepare statement
    C.prepare("searchstmt", "SELECT name FROM employee WHERE id=$1;");
    // invocation as in variable binding
    pqxx::result R = txn2.prepared("searchstmt")(1).exec();
    txn2.commit();

    // test prepared statement already in statement cache
    // LOG_INFO("[Prepare statement cache]
    // %d",conn->pkt_manager.ExistCachedStatement("searchstmt"));
    EXPECT_EQ(R.size(), 1);
    EXPECT_TRUE(conn->pkt_manager.ExistCachedStatement("searchstmt"));
  } catch (const std::exception &e) {
    LOG_INFO("[PrepareStatementTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
=======
    W.exec("DROP TABLE IF EXISTS employee;");
    W.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    W.commit();
    
    pqxx::work W1(C);
    W1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    W1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    W1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    // test prepare statement
    C.prepare("searchstmt","SELECT name FROM employee WHERE id=$1;");
    // invocation as in variable binding
    pqxx::result R = W1.prepared("searchstmt")(1).exec();
    W1.commit();

    // test prepared statement already in statement cache
    // LOG_INFO("[Prepare statement cache] %d",conn->pkt_manager.ExistCachedStatement("searchstmt"));
    EXPECT_TRUE(conn->pkt_manager.ExistCachedStatement("searchstmt"));

    LOG_INFO("Prepare statement search result:%lu",R.size());
  } catch (const std::exception &e) {
    LOG_INFO("[PrepareStatementTest] Exception occurred");
>>>>>>> 353f1b8... Split packet_manager_test into simple_query_test and prepare_stmt_test
  }

  LOG_INFO("[PrepareStatementTest] Client has closed");
  return NULL;
}

TEST_F(PrepareStmtTests, PrepareStatementTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::wire::LibeventServer libeventserver;
  int port = 15721;
<<<<<<< HEAD
  std::thread serverThread(LaunchServer, libeventserver, port);
=======
  std::thread serverThread(LaunchServer, libeventserver,port);
>>>>>>> 353f1b8... Split packet_manager_test into simple_query_test and prepare_stmt_test
  while (!libeventserver.GetIsStarted()) {
    sleep(1);
  }

  PrepareStatementTest(port);

  libeventserver.CloseServer();
  serverThread.join();
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

}  // End test namespace
}  // End peloton namespace
