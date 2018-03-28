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

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "common/harness.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "network/connection_handle_factory.h"
#include "network/peloton_server.h"
#include "network/postgres_protocol_handler.h"
#include "network/protocol_handler_factory.h"
#include "peloton_config.h"
#include "util/string_util.h"

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// SSL TESTS
//===--------------------------------------------------------------------===//

class SSLTests : public PelotonTest {};

// The following keys and certificates are generated using
// https://www.postgresql.org/docs/9.5/static/libpq-ssl.html
std::string client_crt = std::string(
    StringUtil::Format("%s%s", SOURCE_FOLDER, "/test/network/ssl/root.crt"));
std::string client_key = std::string(
    StringUtil::Format("%s%s", SOURCE_FOLDER, "/test/network/ssl/root.key"));
std::string server_crt = std::string(
    StringUtil::Format("%s%s", SOURCE_FOLDER, "/test/network/ssl/server.crt"));
std::string server_key = std::string(
    StringUtil::Format("%s%s", SOURCE_FOLDER, "/test/network/ssl/server.key"));
std::string root_crt = std::string(
    StringUtil::Format("%s%s", SOURCE_FOLDER, "/test/network/ssl/root.crt"));

/**
 * Basic SSL connection test:  Tested with valid certificats and key files
 */
void *TestRoutine(int port) {
  try {
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database application_name=psql "
        "sslmode=require sslkey=%s sslcert=%s sslrootcert=%s",
        port, client_key.c_str(), client_crt.c_str(), root_crt.c_str()));

    pqxx::work txn1(C);

    peloton::network::ConnectionHandle *conn =
        peloton::network::ConnectionHandleFactory::GetInstance()
            .ConnectionHandleAt(peloton::network::PelotonServer::recent_connfd)
            .get();

    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler *>(
            conn->GetProtocolHandler().get());
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
  EXPECT_EQ(0, chmod(client_key.c_str(), S_IRUSR));
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::network::PelotonServer peloton_server;
  int port = 15721;
  try {
    peloton::network::PelotonServer::certificate_file_ = server_crt;
    peloton::network::PelotonServer::private_key_file_ = server_key;
    peloton::network::PelotonServer::root_cert_file_ = root_crt;
    peloton::network::PelotonServer::SSLInit();

    peloton_server.SetPort(port);
    peloton_server.SetupServer();
  } catch (peloton::ConnectionException &exception) {
    LOG_INFO("[LaunchServer] exception in thread");
  }

  std::thread serverThread([&] { peloton_server.ServerLoop(); });

  // server & client running correctly
  TestRoutine(port);

  peloton_server.Close();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton
