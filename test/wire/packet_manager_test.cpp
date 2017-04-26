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
#include "util/string_util.h"
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Packet Manager Tests
//===--------------------------------------------------------------------===//

class PacketManagerTests : public PelotonTest {};

static void *LaunchServer(peloton::wire::LibeventServer libeventserver,int port) {
  try {
	libeventserver.SetPort(port);
    libeventserver.StartServer();
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
        "host=127.0.0.1 port=%d user=postgres sslmode=disable",port));
    LOG_INFO("[SimpleQueryTest] Connected to %s", C.dbname());
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
    W.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    W.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    W.commit();
    
    pqxx::result R = W.exec("SELECT name FROM employee where id=1;");

    EXPECT_EQ(R.size(), 1);
    LOG_INFO("[SimpleQueryTest] Found %lu employees", R.size());
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

void *PrepareStatementTest(int port) {
  try {
    pqxx::connection C(StringUtil::Format(
            "host=127.0.0.1 port=%d user=postgres sslmode=disable",port));
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
    // LOG_INFO("[Prepare statement cache] %d",conn->pkt_manager.ExistCachedStatement("searchstmt"));
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
 * YINGJUN: rewrite wanted.
 */
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

/**
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */

TEST_F(PacketManagerTests, SimpleQueryTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::wire::LibeventServer libeventserver;

  int port = 15721;
  std::thread serverThread(LaunchServer, libeventserver,port);
  while (!libeventserver.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  SimpleQueryTest(port);

  libeventserver.CloseServer();
  serverThread.join();
  LOG_INFO("Thread has joined");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

TEST_F(PacketManagerTests, PrepareStatementTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::wire::LibeventServer libeventserver;
  int port = 15721;
  std::thread serverThread(LaunchServer, libeventserver,port);
  while (!libeventserver.GetIsStarted()) {
    sleep(1);
  }

  PrepareStatementTest(port);

  libeventserver.CloseServer();
  serverThread.join();
  LOG_INFO("Thread has joined");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
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
