//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// sample_test.cpp
//
// Identification: tests/common/sample_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include "backend/logging/logmanager.h"

#include <thread>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Simple Logging Test 
//===--------------------------------------------------------------------===//

TEST(SimpleLoggingTest, aries_logging_test) {
  auto& logManager = logging::LogManager::GetInstance();

  std::thread thread(&peloton::logging::LogManager::StandbyLogging,
                     &logManager,
                     peloton::LOGGING_TYPE_ARIES);

  // bootstrap
  // create db, etc.

  logManager.StartLogging();

  std::terminate();
  
}

}  // End test namespace
}  // End peloton namespace
