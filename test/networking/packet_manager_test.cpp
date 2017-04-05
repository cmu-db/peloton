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
#include <pqxx/pqxx>

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Packet Manager Tests
//===--------------------------------------------------------------------===//

class PacketManagerTests : public PelotonTest {};

static void *LaunchServer(peloton::wire::LibeventServer libeventserver) {
  try {
    // Setup
    // peloton::PelotonInit::Initialize();
    // LOG_INFO("Server initialized\n");
    // Launch server
    // peloton::wire::LibeventServer libeventserver;
    
    libeventserver.StartServer();
    LOG_INFO("Server Closed\n");
    // Teardown
    // Todo: Peloton cannot shut down normally, will try to fix this soon
    // peloton::PelotonInit::Shutdown();
    // LOG_INFO("Peloton has shut down\n");
  } catch (peloton::ConnectionException exception) {
    // Nothing to do here!
  }
  return NULL;
}


static void *SimpleQueryTest(void *) {
  try {
    pqxx::connection C(
        "host=127.0.0.1 port=15721 user=postgres sslmode=disable");
    LOG_INFO("Connected to %s\n", C.dbname());
    pqxx::work W(C);

    // create table and insert some data
    W.exec("DROP TABLE IF EXISTS employee;");
    W.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    W.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    W.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    W.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    pqxx::result R = W.exec("SELECT name FROM employee where id=1;");

    EXPECT_EQ(R.size(), 1);
    LOG_INFO("Found %lu employees\n", R.size());
    W.commit();
  } catch (const std::exception &e) {
    LOG_INFO("Exception occurred\n");
  }

  LOG_INFO("Client has closed\n");
  return NULL;
}


TEST_F(PacketManagerTests, SimpleQueryTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized\n");
  // pthread_t threads[NUM_THREADS];
  peloton::wire::LibeventServer libeventserver;
  std::thread serverThread(LaunchServer, libeventserver);

  sleep(5);

  SimpleQueryTest(NULL);

  // pthread_kill(threads[0], SIGHUP);
  libeventserver.CloseServer();
  serverThread.join();
}

}  // End test namespace
}  // End peloton namespace
