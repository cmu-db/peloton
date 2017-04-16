//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// packet_manager_test.cpp
//
// Identification: test/wire/packet_manager_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"
#include "common/logger.h"
#include "wire/libevent_server.h"
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Packet Manager Tests
//===--------------------------------------------------------------------===//

class PacketManagerTests : public PelotonTest {};

static void *LaunchServer(peloton::wire::LibeventServer libeventserver) {
  try {
    libeventserver.StartServer();
  } catch (peloton::ConnectionException exception) {
    LOG_INFO("[LaunchServer] exception in thread");
  }
  return NULL;
}

/**
 * Simple select query test
 */
static void *SimpleQueryTest(void *) {
  try {
    pqxx::connection C(
        "host=127.0.0.1 port=15721 user=postgres sslmode=disable");
    LOG_INFO("[SimpleQueryTest] Connected to %s", C.dbname());
    pqxx::work W(C);

    peloton::wire::LibeventSocket *conn =
        peloton::wire::LibeventServer::GetConn(
            peloton::wire::LibeventServer::recent_connfd);

    EXPECT_EQ(conn->pkt_manager.is_started, true);
    EXPECT_EQ(conn->state, peloton::wire::CONN_READ);
    // create table and insert some data
    W.exec("DROP TABLE IF EXISTS employee;");
    W.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    W.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    W.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    W.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    pqxx::result R = W.exec("SELECT name FROM employee where id=1;");

    EXPECT_EQ(R.size(), 1);
    LOG_INFO("[SimpleQueryTest] Found %lu employees", R.size());
    W.commit();
  } catch (const std::exception &e) {
    LOG_INFO("[SimpleQueryTest] Exception occurred");
  }

  LOG_INFO("[SimpleQueryTest] Client has closed");
  return NULL;
}

/**
 * named prepare statement without parameters
 * TODO: add prepare's parameters when parser team fix the bug
 */

static void *PrepareStatementTest(void *) {
  try {
    pqxx::connection C(
        "host=127.0.0.1 port=15721 user=postgres sslmode=disable");
    LOG_INFO("[PrepareStatementTest] Connected to %s", C.dbname());
    pqxx::work W(C);
    
    peloton::wire::LibeventSocket *conn =
        peloton::wire::LibeventServer::GetConn(
            peloton::wire::LibeventServer::recent_connfd);

    // create table and insert some data
    W.exec("DROP TABLE IF EXISTS employee;");
    W.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    W.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    W.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    W.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    // test prepare statement
    C.prepare("searchstmt","SELECT name FROM employee WHERE id=1;");
    // invocation as in variable binding
    pqxx::result R = W.prepared("searchstmt").exec();
    W.commit();

    // test prepared statement already in statement cache
//    LOG_INFO("[Prepare statement cache] %d",conn->pkt_manager.ExistCachedStatement("searchstmt"));
    EXPECT_TRUE(conn->pkt_manager.ExistCachedStatement("searchstmt"));

    LOG_INFO("Prepare statement search result:%lu",R.size());
  } catch (const std::exception &e) {
    LOG_INFO("[PrepareStatementTest] Exception occurred");
  }

  LOG_INFO("[PrepareStatementTest] Client has closed");
  return NULL;
}

/**
 * rollback test
 */
static void *RollbackTest(void *) {
  try {
    pqxx::connection C(
        "host=127.0.0.1 port=15721 user=postgres sslmode=disable");
    LOG_INFO("[RollbackTest] Connected to %s", C.dbname());
    pqxx::work W(C);

    peloton::wire::LibeventSocket *conn =
        peloton::wire::LibeventServer::GetConn(
            peloton::wire::LibeventServer::recent_connfd);

    EXPECT_EQ(conn->pkt_manager.is_started, true);
    EXPECT_EQ(conn->state, peloton::wire::CONN_READ);
    // create table and insert some data
    W.exec("DROP TABLE IF EXISTS employee;");
    W.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    W.abort();

    // W.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    // W.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    // W.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    pqxx::result R = W.exec("SELECT name FROM employee where id=1;");

    EXPECT_EQ(R.size(), 1);
    LOG_INFO("[RollbackTest] Found %lu employees", R.size());
    W.commit();
  } catch (const std::exception &e) {
    LOG_INFO("[RollbackTest] Exception occurred");
  }

  LOG_INFO("[RollbackTest] Client has closed");
  return NULL;
}


// /**
//  * Use std::thread to initiate peloton server and pqxx client in separate
//  * threads
//  * Simple query test to guarantee both sides run correctly
//  * Callback method to close server after client finishes
//  */
// TEST_F(PacketManagerTests, SimpleQueryTest) {
//   peloton::PelotonInit::Initialize();
//   LOG_INFO("Server initialized");
//   peloton::wire::LibeventServer libeventserver;
//   std::thread serverThread(LaunchServer, libeventserver);
//   while (!libeventserver.is_started) {
//     sleep(1);
//   }

//   /* server & client running correctly */
//   SimpleQueryTest(NULL);

//   /* TODO: monitor packet_manager's status when receiving prepare statement from
//    * client */
//   // PrepareStatementTest(NULL);

//   libeventserver.CloseServer();
//   serverThread.join();
//   LOG_INFO("Thread has joined");
//   peloton::PelotonInit::Shutdown();
//   LOG_INFO("Peloton has shut down\n");
// }

// TEST_F(PacketManagerTests, PrepareStatementTest) {
//   peloton::PelotonInit::Initialize();
//   LOG_INFO("Server initialized");
//   peloton::wire::LibeventServer libeventserver;
//   std::thread serverThread(LaunchServer, libeventserver);
//   while (!libeventserver.is_started) {
//     sleep(1);
//   }

//   PrepareStatementTest(NULL);

//   libeventserver.CloseServer();
//   serverThread.join();
//   LOG_INFO("Thread has joined");
//   peloton::PelotonInit::Shutdown();
//   LOG_INFO("Peloton has shut down\n");
// }


TEST_F(PacketManagerTests, RollbackTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::wire::LibeventServer libeventserver;
  std::thread serverThread(LaunchServer, libeventserver);
  while (!libeventserver.is_started) {
    sleep(1);
  }

  RollbackTest(NULL);

  libeventserver.CloseServer();
  serverThread.join();
  LOG_INFO("Thread has joined");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down\n");
}

}  // End test namespace
}  // End peloton namespace
