//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// storage_manager_test.cpp
//
// Identification: tests/common/storage_manager_test.cpp
//
// Copyright (c) 201CACHE_SIZE, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"
#include "backend/storage/storage_manager.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Storage Manager Test
//===--------------------------------------------------------------------===//

/**
 * Test basic functionality
 *
 */
TEST(StorageManagerTests, BasicTest) {

  peloton::storage::StorageManager storage_manager;

  std::vector<peloton::BackendType> backend_types = {peloton::BACKEND_TYPE_MM, peloton::BACKEND_TYPE_FILE };

  size_t length = 256;
  size_t rounds = 100;

  for(auto backend_type : backend_types) {
    std::cout << "Backend :: " << backend_type << "\n";

    for(size_t round_itr = 0; round_itr < rounds ; round_itr++) {

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
