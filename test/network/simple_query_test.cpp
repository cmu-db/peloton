//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_query_test.cpp
//
// Identification: test/network/simple_query_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"
#include "common/logger.h"
#include "network/network_manager.h"
#include "network/protocol_handler_factory.h"
#include "util/string_util.h"
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "network/postgres_protocol_handler.h"

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Simple Query Tests
//===--------------------------------------------------------------------===//

class SimpleQueryTests : public PelotonTest {};

static void *LaunchServer(peloton::network::NetworkManager network_manager,
                          int port) {
  try {
    network_manager.SetPort(port);
    network_manager.StartServer();
  } catch (peloton::ConnectionException &exception) {
    LOG_INFO("[LaunchServer] exception in thread");
  }
  return NULL;
}

/**
 * Simple select query test
 */
void *SimpleQueryTest(int port) {
  try {
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=postgres sslmode=disable application_name=psql", port));
    pqxx::work txn1(C);

    peloton::network::NetworkConnection *conn =
        peloton::network::NetworkManager::GetConnection(
            peloton::network::NetworkManager::recent_connfd);

    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler*>(conn->protocol_handler_.get());
    EXPECT_NE(handler, nullptr);

    // EXPECT_EQ(conn->state, peloton::network::CONN_READ);
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

    pqxx::work txn3(C);
    txn3.exec("DROP TABLE IF EXISTS foo;");
    txn3.exec("CREATE TABLE foo(length DECIMAL);");
    txn3.commit();

    pqxx::work txn4(C);
    txn4.exec("PREPARE func AS INSERT INTO foo VALUES($1);");
    txn4.exec("EXECUTE func(1);");
    txn4.exec("EXECUTE func(1+1);");
    txn4.exec("EXECUTE func(SQRT(9.0));");

    pqxx::result R2 = txn4.exec("SELECT * FROM foo;");
    txn4.commit();
    EXPECT_EQ(R2.size(), 3);

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

    peloton::network::NetworkConnection *conn =
        peloton::network::NetworkManager::GetConnection(
            peloton::network::NetworkManager::recent_connfd);

    EXPECT_TRUE(conn->protocol_handler_.is_started);
    // EXPECT_EQ(conn->state, peloton::network::CONN_READ);
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
  peloton::network::NetworkManager network_manager;
  std::thread serverThread(LaunchServer, network_manager,port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  RollbackTest(port);

  network_manager.CloseServer();
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
  peloton::network::NetworkManager network_manager;

  int port = 15721;
  std::thread serverThread(LaunchServer, network_manager, port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  SimpleQueryTest(port);

  network_manager.CloseServer();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
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
//  /* launch 2 network managers in different port */
//  // first server
//  int port1 = 15721;
//  peloton::network::NetworkManager network_manager1;
//  std::thread serverThread1(LaunchServer, network_manager1,port1);
//
//  // second server
//  int port2 = 15722;
//  peloton::network::NetworkManager network_manager2;
//  std::thread serverThread2(LaunchServer, network_manager2,port2);
//
//  while (!network_manager1.is_started || !network_manager2.is_started) {
//    sleep(1);
//  }
//
//  /* launch 2 clients to do simple query separately */
//  SimpleQueryTest(port1);
//  SimpleQueryTest(port2);
//
//  network_manager1.CloseServer();
//  network_manager2.CloseServer();
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
