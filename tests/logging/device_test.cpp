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
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "backend/common/timer.h"
#include "backend/common/types.h"
#include "backend/common/logger.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Device Test
//===--------------------------------------------------------------------===//

class DeviceTest : public PelotonTest {};

#define DATA_FILE_LEN  1024 * 1024 * UINT64_C(512)  // 512 MB
#define DATA_FILE_NAME "peloton.pmem"
#define DATA_ALIGNMENT 4096

#define NUM_READ_TRIALS     10000
#define NUM_WRITE_TRIALS    1000

#define roundup2(x, y)  (((x)+((y)-1))&(~((y)-1))) /* if y is powers of two */

std::size_t GetFileOffset(const std::size_t max_chunk_size){
  assert(DATA_FILE_LEN > max_chunk_size);
  std::size_t offset = 0;

  while(1){
    // Get a random offset
    offset = rand() % DATA_FILE_LEN;
    offset = roundup2(offset, DATA_ALIGNMENT);

    if(offset + max_chunk_size < DATA_FILE_LEN) {
      break;
    }
  }

  return offset;
}

Timer<> timer;

void ReadTest(const int data_fd,
              const oid_t chunk_size_itr,
              const size_t max_chunk_size,
              void* buffer,
              const bool random) {
  int result;
  double average_time;

  // Figure out offset
  ssize_t offset = GetFileOffset(max_chunk_size);

  // Compute chunk size
  std::size_t chunk_size = pow(2, chunk_size_itr);

  // Figure out # of trials
  oid_t num_trials = NUM_READ_TRIALS;

  // Clean up buffer
  bzero(buffer, max_chunk_size);

  // Reset timer
  timer.Reset();

  // READS
  for(oid_t trial_itr = 0; trial_itr < num_trials; trial_itr++) {
    // Compute a random offset if needed
    if(random == true) {
      offset = GetFileOffset(max_chunk_size);
    }

    // Start timer
    timer.Start();

    result = pread(data_fd, buffer, chunk_size, offset);
    if(result == -1){
      LOG_ERROR("pread");
      return;
    }

    // Stop timer
    timer.Stop();
  }


  // Set duration
  average_time = timer.GetDuration() / num_trials;
  std::cout << "Chunk size : " << chunk_size << " Average READ  time : " << average_time * 1000 << "\n";
}

void WriteTest(const int data_fd,
               const oid_t chunk_size_itr,
               const size_t max_chunk_size,
               void* buffer,
               const bool random) {
  int result;
  double average_time;

  // Figure out offset
  ssize_t offset = GetFileOffset(max_chunk_size);

  // Compute chunk size
  std::size_t chunk_size = pow(2, chunk_size_itr);

  // Figure out # of trials
  oid_t num_trials = NUM_WRITE_TRIALS;

  // Set up buffer
  memset(buffer, 'f', max_chunk_size);

  // Reset timer
  timer.Reset();

  // WRITES
  for(oid_t trial_itr = 0; trial_itr < num_trials; trial_itr++) {
    // Compute a random offset if needed
    if(random == true) {
      offset = GetFileOffset(max_chunk_size);
    }

    // Start timer
    timer.Start();

    result = pwrite(data_fd, buffer, chunk_size, offset);
    if(result == -1){
      LOG_ERROR("pwrite");
      return;
    }

    // Stop timer
    timer.Stop();

  }

  // Set duration
  average_time = timer.GetDuration() / num_trials;
  std::cout << "Chunk size : " << chunk_size << " Average WRITE time : " << average_time * 1000 << "\n";
}

TEST_F(DeviceTest, BenchmarkTest) {

  std::vector<std::string> data_file_dirs = {NVM_DIR, HDD_DIR};
  int data_fd;
  std::size_t begin_chunk_size = 9, end_chunk_size = 21; // lg base 2

  // Go over all the dirs
  for(auto data_file_dir : data_file_dirs){

    // Create a data file
    std::string data_file_name = data_file_dir + DATA_FILE_NAME;
    std::cout << "--------------------------------------------------\n\n";
    std::cout << "Data File Name : " << data_file_name << "\n";
    std::cout << "Data File Len  : " << DATA_FILE_LEN << "\n";

    if ((data_fd = open(data_file_name.c_str(), O_CREAT | O_RDWR | O_DIRECT | O_SYNC, 0666)) < 0) {
      LOG_ERROR("%s: No such file or directory", data_file_name.c_str());
      return;
    }

    // Allocate the data file
    if ((errno = posix_fallocate(data_fd, 0, DATA_FILE_LEN)) != 0) {
      LOG_ERROR("%s: posix_fallocate", data_file_name.c_str());
      return;
    }

    std::size_t max_chunk_size = pow(2, end_chunk_size);
    void *buffer;
    int result;
    oid_t chunk_size_interval = 4;

    // Randomize stuff
    srand(time(NULL));

    // Allocate the aligned buffer
    result = posix_memalign(&buffer, DATA_ALIGNMENT, max_chunk_size);
    if (result == -1) {
      LOG_ERROR("memalign");
      return;
    }

    std::cout << "\nSEQUENTIAL READ \n\n";

    // SEQUENTIAL READ
    for(oid_t chunk_size_itr = begin_chunk_size;
        chunk_size_itr <= end_chunk_size;
        chunk_size_itr += chunk_size_interval){

      ReadTest(data_fd, chunk_size_itr, max_chunk_size, buffer, false);

    }

    std::cout << "\nSEQUENTIAL WRITE \n\n";

    // SEQUENTIAL WRITE
    for(oid_t chunk_size_itr = begin_chunk_size;
        chunk_size_itr <= end_chunk_size;
        chunk_size_itr += chunk_size_interval){

      WriteTest(data_fd, chunk_size_itr, max_chunk_size, buffer, false);

    }

    std::cout << "\nRANDOM READ \n\n";

    // RANDOM READ
    for(oid_t chunk_size_itr = begin_chunk_size;
        chunk_size_itr <= end_chunk_size;
        chunk_size_itr += chunk_size_interval){

      ReadTest(data_fd, chunk_size_itr, max_chunk_size, buffer, true);

    }

    std::cout << "\nRANDOM WRITE \n\n";

    // RANDOM WRITE
    for(oid_t chunk_size_itr = begin_chunk_size;
        chunk_size_itr <= end_chunk_size;
        chunk_size_itr += chunk_size_interval){

      WriteTest(data_fd, chunk_size_itr, max_chunk_size, buffer, true);

    }


    // Close the pmem file
    close(data_fd);

    free(buffer);
  }

}

}  // End test namespace
}  // End peloton namespace

