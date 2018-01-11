//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// exception_test.cpp
//
// Identification: test/network/exception_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include <pqxx/except.hxx>

#include "common/harness.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "network/network_manager.h"
#include "network/protocol_handler_factory.h"
#include "network/postgres_protocol_handler.h"
#include "util/string_util.h"

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Exception Test
//===--------------------------------------------------------------------===//

class ExceptionTests : public PelotonTest {};

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

/*
 * To test with the queries with syntax error that will be caught by parser.
 * The server will catch these errors in Networking layer and directly return ERROR response.
 */
void *ParserExceptionTest(int port) {
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

    // If an exception occurs on one transaction, we can not use this transaction anymore
    int exception_count = 0, total = 6;

    // DROP query
    pqxx::work txn1(C);
    try {
      txn1.exec("DROP TABEL IF EXISTS employee;");
      txn1.commit();
    } catch (const pqxx::pqxx_exception &e) {
      const pqxx::sql_error *s = dynamic_cast<const pqxx::sql_error*>(&e.base());
      if (s) {
        LOG_TRACE("Invalid Drop Query: %s", e.base().what());
        exception_count += 1;
      }
    }

    // CREATE query
    pqxx::work txn2(C);
    try {
      txn2.exec("CREATE TABEL employee(id ITN, name VARCHAR(100));");
      txn2.commit();
    } catch (const pqxx::pqxx_exception &e) {
      const pqxx::sql_error *s = dynamic_cast<const pqxx::sql_error*>(&e.base());
      if (s) {
        LOG_TRACE("Invalid Create Query: %s", e.base().what());
        exception_count += 1;
      }
    }
    pqxx::work txn3(C);
    txn3.exec("DROP TABLE IF EXISTS foo;");
    txn3.exec("CREATE TABLE foo(id INT);");
    txn3.commit();

    // Select query
    pqxx::work txn4(C);
    try {
      txn4.exec("SELECT name FROM foo id = 1;");
      txn4.commit();
    } catch (const pqxx::pqxx_exception &e) {
      const pqxx::sql_error *s = dynamic_cast<const pqxx::sql_error*>(&e.base());
      if (s) {
        LOG_TRACE("Invalid Select Query: %s", e.base().what());
        exception_count += 1;
      }
    }

    // SELECT;
    pqxx::work txn5(C);
    try {
      txn5.exec("SELECT ;");
      txn5.commit();
    } catch (const pqxx::pqxx_exception &e) {
      const pqxx::sql_error *s = dynamic_cast<const pqxx::sql_error*>(&e.base());
      if (s) {
        LOG_TRACE("Invalid Select Query: %s", e.base().what());
        exception_count += 1;
      }
    }

    // Prepare query
    pqxx::work txn6(C);
    try {
      txn6.exec("PREPARE func INSERT INTO foo VALUES($1, $2);");
      txn6.commit();
    } catch (const pqxx::pqxx_exception &e) {
      const pqxx::sql_error *s = dynamic_cast<const pqxx::sql_error*>(&e.base());
      if (s) {
        LOG_TRACE("Invalid Prepare Query: %s", e.base().what());
        exception_count += 1;
      }
    }

    // Execute query
    pqxx::work txn7(C);
    try {
      txn7.exec("PREPARE func(INT) AS INSERT INTO foo VALUES($1);");
      txn7.exec("EXECUTE fun;");
      txn7.commit();
    } catch (const pqxx::pqxx_exception &e) {
      const pqxx::sql_error *s = dynamic_cast<const pqxx::sql_error*>(&e.base());
      if (s) {
        LOG_TRACE("Invalid Execute Query: %s", e.base().what());
        exception_count += 1;
      }
    }

    // Empty query, should be valid
    pqxx::work txn8(C);
    try {
      txn8.exec(";;");
      txn8.commit();
    } catch (const pqxx::pqxx_exception &e) {
      const pqxx::sql_error *s = dynamic_cast<const pqxx::sql_error*>(&e.base());
      if (s) {
        LOG_TRACE("Empty Query: %s", e.base().what());
        EXPECT_TRUE(false);
      }
    }

    // Check the number of exceptions
    EXPECT_EQ(exception_count, total);

  } catch (const std::exception &e) {
    LOG_ERROR("[ExceptionTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }

  LOG_INFO("[ExceptionTest] Client has closed");
  return NULL;
}

/**
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */
TEST_F(ExceptionTests, ParserExceptionTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::network::NetworkManager network_manager;

  int port = 15721;
  std::thread serverThread(LaunchServer, network_manager, port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  ParserExceptionTest(port);

  network_manager.CloseServer();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton
