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
#include "catalog/catalog.h"
#include "catalog/manager.h"
#include "util/string_util.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/index_catalog.h"
#include "concurrency/transaction_manager_factory.h"

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include <include/network/postgres_protocol_handler.h>

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Simple Query Tests
//===--------------------------------------------------------------------===//

class RecoveryTests : public PelotonTest {};

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
void *StartTest(int port) {
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
    txn1.exec("CREATE TABLE employee(id INT PRIMARY KEY, name VARCHAR(100));");
    txn1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");
    txn1.commit();

  } catch (const std::exception &e) {
    LOG_INFO("[SimpleQueryTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }

  LOG_INFO("[SimpleQueryTest] Client has closed");
  return NULL;
}

void *RecoveryTest(int port) {
  try {
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=postgres sslmode=disable application_name=psql", port));
    peloton::network::NetworkConnection *conn =
        peloton::network::NetworkManager::GetConnection(
            peloton::network::NetworkManager::recent_connfd);

    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler*>(conn->protocol_handler_.get());
    EXPECT_NE(handler, nullptr);

    pqxx::work txn2(C);
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
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */
TEST_F(RecoveryTests, SetupQueryTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::network::NetworkManager network_manager;

  int port = 15721;
  std::thread serverThread(LaunchServer, network_manager, port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  StartTest(port);

  network_manager.CloseServer();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  catalog::Catalog::GetInstance()->DropIndex(50331763, concurrency::TransactionManagerFactory::GetInstance().BeginTransaction(IsolationLevelType::SERIALIZABLE));
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

TEST_F(RecoveryTests, RecoveryQueryTest) {

  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::network::NetworkManager network_manager;
  //delete catalog::Manager::GetInstance();
  int port = 15721;
  std::thread serverThread(LaunchServer, network_manager, port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  RecoveryTest(port);

  network_manager.CloseServer();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  remove("/tmp/log/log_0_0");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}


}  // namespace test
}  // namespace peloton
