//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_query_test.cpp
//
// Identification: test/networking/simple_query_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"
#include "common/logger.h"
#include "networking/network_server.h"
#include "util/string_util.h"
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Simple Query Tests
//===--------------------------------------------------------------------===//

class SimpleQueryTests : public PelotonTest {};

static void *LaunchServer(peloton::networking::NetworkServer networkserver,
                          int port) {
  try {
    networkserver.SetPort(port);
    networkserver.StartServer();
  } catch (peloton::ConnectionException exception) {
    LOG_INFO("[LaunchServer] exception in thread");
  }
  return NULL;
}

/**
 * Simple select query test
 */
void *SimpleQueryTest(int port) {
  try {
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=postgres sslmode=disable", port));
    LOG_INFO("[SimpleQueryTest] Connected to %s", C.dbname());
    pqxx::work txn1(C);

    peloton::networking::NetworkSocket *conn =
        peloton::networking::NetworkServer::GetConn(
            peloton::networking::NetworkServer::recent_connfd);

    EXPECT_EQ(conn->pkt_manager.is_started, true);
    // EXPECT_EQ(conn->state, peloton::wire::CONN_READ);
    // create table and insert some data
    txn1.exec("DROP TABLE IF EXISTS employee;");
    txn1.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    txn1.commit();

    pqxx::work txn2(C);
    txn2.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn2.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn2.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    pqxx::result R = txn2.exec("SELECT name FROM employee where id=1;");
    txn2.commit();

    EXPECT_EQ(R.size(), 1);
  } catch (const std::exception &e) {
    LOG_INFO("[SimpleQueryTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }

  LOG_INFO("[SimpleQueryTest] Client has closed");
  return NULL;
}

/**
 * rollback test
 * YINGJUN: rewrite wanted.
 */
/*
void *RollbackTest(int port) {
  try {
    pqxx::connection C(StringUtil::Format(
            "host=127.0.0.1 port=%d user=postgres sslmode=disable",port));
    LOG_INFO("[RollbackTest] Connected to %s", C.dbname());
    pqxx::work W(C);

    peloton::wire::LibeventSocket *conn =
        peloton::wire::LibeventServer::GetConn(
            peloton::wire::LibeventServer::recent_connfd);

    EXPECT_EQ(conn->pkt_manager.is_started, true);
    // EXPECT_EQ(conn->state, peloton::wire::CONN_READ);
    // create table and insert some data
    W.exec("DROP TABLE IF EXISTS employee;");
    W.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    W.exec("INSERT INTO employee VALUES (1, 'Han LI');");

    W.abort();

    W.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    W.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    W.commit();


    // pqxx::result R = W.exec("SELECT name FROM employee where id=1;");

    // EXPECT_EQ(R.size(), 1);

    // LOG_INFO("[RollbackTest] Found %lu employees", R.size());
    // W.commit();

  } catch (const std::exception &e) {
    LOG_INFO("[RollbackTest] Exception occurred");
  }

  LOG_INFO("[RollbackTest] Client has closed");
  return NULL;
}
*/

/*
TEST_F(PacketManagerTests, RollbackTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  int port = 15721;
  peloton::wire::LibeventServer libeventserver;
  std::thread serverThread(LaunchServer, libeventserver,port);
  while (!libeventserver.GetIsStarted()) {
    sleep(1);
  }

  RollbackTest(port);

  libeventserver.CloseServer();
  serverThread.join();
  LOG_INFO("Thread has joined");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}
*/

/**
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */
TEST_F(SimpleQueryTests, SimpleQueryTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::networking::NetworkServer networkserver;

  int port = 15721;
  std::thread serverThread(LaunchServer, networkserver, port);
  while (!networkserver.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  SimpleQueryTest(port);

  networkserver.CloseServer();
  serverThread.join();
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

///**
// * Scalability test
// * Open 2 servers in threads concurrently
// * Both conduct simple query job
// */
// TEST_F(PacketManagerTests, ScalabilityTest) {
//  peloton::PelotonInit::Initialize();
//
//  /* launch 2 libevent servers in different port */
//  // first server
//  int port1 = 15721;
//  peloton::wire::LibeventServer libeventserver1;
//  std::thread serverThread1(LaunchServer, libeventserver1,port1);
//
//  // second server
//  int port2 = 15722;
//  peloton::wire::LibeventServer libeventserver2;
//  std::thread serverThread2(LaunchServer, libeventserver2,port2);
//
//  while (!libeventserver1.is_started || !libeventserver2.is_started) {
//    sleep(1);
//  }
//
//  /* launch 2 clients to do simple query separately */
//  SimpleQueryTest(port1);
//  SimpleQueryTest(port2);
//
//  libeventserver1.CloseServer();
//  libeventserver2.CloseServer();
//
//  serverThread1.join();
//  serverThread2.join();
//
//  LOG_INFO("[ScalabilityTest] Threads have joined");
//  peloton::PelotonInit::Shutdown();
//  LOG_INFO("[ScalabilityTest] Peloton has shut down");
//}

}  // namespace test
}  // namespace peloton
