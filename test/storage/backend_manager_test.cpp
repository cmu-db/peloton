//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_manager_test.cpp
//
// Identification: test/storage/backend_manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "storage/backend_manager.h"

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
  peloton::storage::BackendManager backend_manager;

  std::vector<BackendType> backend_types = {BackendType::MM};

  size_t length = 256;
  size_t rounds = 100;

  for (auto backend_type : backend_types) {
    LOG_INFO("Backend :: %s", BackendTypeToString(backend_type).c_str());

    for (size_t round_itr = 0; round_itr < rounds; round_itr++) {
      // Allocate
      auto location = backend_manager.Allocate(backend_type, length);

      // Fill it up
      PELOTON_MEMSET(location, '-', length);

      // Sync
      backend_manager.Sync(backend_type, location, length);

      // Release
      backend_manager.Release(backend_type, location);
    }
  }
}

}  // namespace test
}  // namespace peloton
