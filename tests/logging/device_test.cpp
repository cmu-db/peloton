//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// device_test.cpp
//
// Identification: tests/logging/device_test.cpp
//
// Copyright (c) 201CACHE_SIZE, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "harness.h"

#include <iostream>
#include <fcntl.h>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <cassert>
#include <getopt.h>
#include <cstdint>

#include "backend/common/timer.h"
#include "backend/storage/storage_manager.h"

// Logging mode
extern LoggingType     peloton_logging_mode;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Device Test
//===--------------------------------------------------------------------===//

class DeviceTest : public PelotonTest {};

#define CACHELINE_SIZE 64

#define roundup2(x, y)  (((x)+((y)-1))&(~((y)-1))) /* if y is powers of two */

TEST_F(DeviceTest, BenchmarkTest) {

  peloton_logging_mode = LOGGING_TYPE_HDD_HDD;

  auto &storage_manager = storage::StorageManager::GetInstance();

  size_t chunk_size = 1024;
  std::string source_buf(chunk_size, 'a');

  auto data = reinterpret_cast<char *>(storage_manager.Allocate(BACKEND_TYPE_HDD,
                                                                chunk_size));

  EXPECT_NE(data, nullptr);

  std::memcpy(data, source_buf.c_str(), chunk_size);

  storage_manager.Sync(BACKEND_TYPE_HDD, data, 0);
}

}  // End test namespace
}  // End peloton namespace

