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
//#include <pthread.h>

#define NUM_THREADS 2

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Packet Manager Tests
//===--------------------------------------------------------------------===//

class PacketManagerTests: public PelotonTest {
};

static void* LaunchServer(void *) {
	// Launch peloton server
	LOG_INFO("Will launch server!\n");
	try {
		// Setup
		peloton::PelotonInit::Initialize();
		LOG_INFO("Server initialized\n");
		// Launch server
		peloton::wire::LibeventServer libeventserver;
		LOG_INFO("Server Closed\n");
		// Teardown
		peloton::PelotonInit::Shutdown();
		LOG_INFO("Peloton has shut down\n");
	} catch (peloton::ConnectionException exception) {
		// Nothing to do here!
	}
	return NULL;
}

static void* LaunchClient(void *) {
	LOG_INFO("Will launch client!\n");
	try {
		pqxx::connection C;
		LOG_INFO("Connected to %s\n", C.dbname());
		pqxx::work W(C);

		// create table and insert some data
		W.exec("DROP TABLE IF EXISTS employee;");
		W.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
		W.exec("INSERT INTO employee VALUES (1, 'Han LI');");
		W.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
		W.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

		LOG_INFO("Test data inserted.\n");

		pqxx::result R = W.exec("SELECT name FROM employee where id=1;");

		LOG_INFO("Found %lu employees\n", R.size());
		W.commit();
	} catch (const std::exception &e) {
		LOG_INFO("Exception occurred\n");
	}

	LOG_INFO("Client has closed\n");
	return NULL;
}

TEST_F(PacketManagerTests, WireInitTest) {
	pthread_t threads[NUM_THREADS];
	int rc = pthread_create(&threads[0], NULL, LaunchServer, NULL);
	if (rc) {
		LOG_INFO("Error:unable to create server thread");
		exit(-1);
	}
	sleep(5);

	LaunchClient(NULL);
	pthread_join(threads[0], NULL);
}

}  // End test namespace
}  // End peloton namespace
