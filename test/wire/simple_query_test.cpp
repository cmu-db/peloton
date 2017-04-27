//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_query_test.cpp
//
// Identification: test/wire/simple_query_test.cpp
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
// Simple Query Tests
//===--------------------------------------------------------------------===//

class SimpleQueryTests : public PelotonTest {};

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
void *SimpleQueryTest(void *) {
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
    W.commit();
    
    pqxx::work W1(C);
    W1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    W1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    W1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    pqxx::result R = W1.exec("SELECT name FROM employee where id=1;");
    W1.commit();

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
 * rollback test
 * YINGJUN: rewrite wanted.
 */
/*
void *RollbackTest(int port) {
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
  peloton::wire::LibeventServer libeventserver;
  std::thread serverThread(LaunchServer, libeventserver,port);
  while (!libeventserver.GetIsStarted()) {
    sleep(1);
  }

  /* server & client running correctly */
  SimpleQueryTest(NULL);

  /* TODO: monitor packet_manager's status when receiving prepare statement from
   * client */
  // PrepareStatementTest(NULL);

  libeventserver.CloseServer();
  serverThread.join();
  LOG_INFO("Thread has joined");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down\n");
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
  peloton::wire::LibeventServer libeventserver;

  int port = 15721;
  std::thread serverThread(LaunchServer, libeventserver,port);
  while (!libeventserver.GetIsStarted()) {
    sleep(1);
  }

  PrepareStatementTest(NULL);

  libeventserver.CloseServer();
  serverThread.join();
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down\n");
}

///**
// * Scalability test
// * Open 2 servers in threads concurrently
// * Both conduct simple query job
// */
//TEST_F(PacketManagerTests, ScalabilityTest) {
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

}  // End test namespace
}  // End peloton namespace
