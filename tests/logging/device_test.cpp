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
#include <string>
#include <vector>

#include "backend/common/timer.h"
#include "backend/common/types.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Device Test
//===--------------------------------------------------------------------===//

class DeviceTest : public PelotonTest {};

#define DATA_FILE_LEN 1024 * 1024 * UINT64_C(512)  // 512 MB
#define DATA_FILE_NAME "peloton.pmem"

TEST_F(DeviceTest, BenchmarkTest) {

  std::vector<std::string> data_file_dirs = {NVM_DIR, HDD_DIR};
  int data_fd;
  size_t data_file_len = DATA_FILE_LEN;
  oid_t num_trials = 3;
  std::size_t begin_chunk_size = 9, end_chunk_size = 21; // lg base 2

  // Go over all the dirs
  for(auto data_file_dir : data_file_dirs){

    // Create a data file
    std::string data_file_name = data_file_dir + DATA_FILE_NAME;
    LOG_INFO("Data File Name : %s", data_file_name.c_str());

    if ((data_fd = open(data_file_name.c_str(), O_CREAT | O_RDWR | O_DIRECT | O_SYNC, 0666)) < 0) {
      LOG_ERROR("%s: No such file or directory", data_file_name.c_str());
      return;
    }

    // Allocate the data file
    if ((errno = posix_fallocate(data_fd, 0, data_file_len)) != 0) {
      LOG_ERROR("%s: posix_fallocate", data_file_name.c_str());
      return;
    }

    // Go over all the chunk sizes
    for(oid_t chunk_size_itr = begin_chunk_size;
        chunk_size_itr <= end_chunk_size;
        chunk_size_itr++){

      // READS
      for(oid_t trial_itr = 0; trial_itr < num_trials; trial_itr++) {

      }

      // WRITES
      for(oid_t trial_itr = 0; trial_itr < num_trials; trial_itr++) {

      }

    }

    // Close the pmem file
    close(data_fd);
  }

}

}  // End test namespace
}  // End peloton namespace

