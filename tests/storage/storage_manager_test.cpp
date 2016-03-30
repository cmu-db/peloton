//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager_test.cpp
//
// Identification: tests/storage/storage_manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include "backend/storage/storage_manager.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Storage Manager Test
//===--------------------------------------------------------------------===//

class StorageManagerTests : public PelotonTest {};

/**
 * Test basic functionality
 *
 */
TEST_F(StorageManagerTests, BasicTest) {
  peloton::storage::StorageManager storage_manager;

  std::vector<peloton::BackendType> backend_types = {peloton::BACKEND_TYPE_MM};

  size_t length = 256;
  size_t rounds = 100;

  for (auto backend_type : backend_types) {
    LOG_INFO("Backend :: %d", backend_type);

    for (size_t round_itr = 0; round_itr < rounds; round_itr++) {
      // Allocate
      auto location = storage_manager.Allocate(backend_type, length);

      // Fill it up
      memset(location, '-', length);

      // Sync
      storage_manager.Sync(backend_type, location, length);

      // Release
      storage_manager.Release(backend_type, location);
    }
  }
}

}  // End test namespace
}  // End peloton namespace
