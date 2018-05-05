//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// recovery_test.cpp
//
// Identification: test/logging/recovery_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "logging/log_buffer.h"
#include "common/harness.h"
#include "gtest/gtest.h"
#include "network/peloton_server.h"
#include "network/postgres_protocol_handler.h"
#include "util/string_util.h"
#include "network/connection_handle_factory.h"

namespace peloton {
  namespace test {

//===--------------------------------------------------------------------===//
// Log Recovery Tests
//===--------------------------------------------------------------------===//

class RecoveryTests : public PelotonTest {};

TEST_F(RecoveryTests, InsertRecoveryTest) {
  
}
}
}