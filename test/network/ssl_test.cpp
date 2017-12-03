//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ssl_test.cpp
//
// Identification: test/network/ssl_test.cpp
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
#include "peloton_config.h"

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// SSL TESTS
//===--------------------------------------------------------------------===//

class SSLTests : public PelotonTest {};

static void *LaunchServer(peloton::network::NetworkManager network_manager,
                          int port) {
  try {
    std::string server_crt = "/test/network/ssl/server_test.crt";
    std::string server_key = "/test/network/ssl/server_test.key";
    std::string root_crt = "/test/network/ssl/root_test.crt";
    peloton::network::NetworkManager::certificate_file_ = SOURCE_FOLDER + server_crt;
    peloton::network::NetworkManager::private_key_file_ = SOURCE_FOLDER + server_key;
    peloton::network::NetworkManager::root_cert_file_ = SOURCE_FOLDER + root_crt;
    peloton::network::NetworkManager::SSLInit();

    network_manager.SetPort(port);
    network_manager.StartServer();
  } catch (peloton::ConnectionException &exception) {
    LOG_INFO("[LaunchServer] exception in thread");
  }
  return NULL;
}

/**
 * Basic SSL connection test:  Tested with valid certificats and key files
 */
void *BasicTest(int port) {
  try {
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=postgres application_name=psql sslmode=require", port));
    pqxx::work txn1(C);

    peloton::network::NetworkConnection *conn =
        peloton::network::NetworkManager::GetConnection(
            peloton::network::NetworkManager::recent_connfd);

    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler*>(conn->protocol_handler_.get());
    EXPECT_NE(handler, nullptr);

    // basic test
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

    // SSL large write test
    pqxx::work txn3(C);
    txn3.exec("DROP TABLE IF EXISTS template;");
    txn3.exec("CREATE TABLE template(id INT);");
    txn3.commit();

    pqxx::work txn4(C);
    for (int i = 0; i < 1000; i++) {
      std::string s = "INSERT INTO template VALUES (" + std::to_string(i) + ")";
      txn4.exec(s);
    }

    R = txn4.exec("SELECT * from template;");
    txn4.commit();

    EXPECT_EQ(R.size(), 1000);

  } catch (const std::exception &e) {
    LOG_INFO("[SSLTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }

  LOG_INFO("[SSLTest] Client has closed");
  return NULL;
}

/**
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */
TEST_F(SSLTests, BasicTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::network::NetworkManager network_manager;

  int port = 15721;
  std::thread serverThread(LaunchServer, network_manager, port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  BasicTest(port);

  network_manager.CloseServer();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton
