//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_manager.h
//
// Identification: src/include/storage/backend_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

#include "common/synchronization/spin_latch.h"
#include "common/internal_types.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Filesystem directories
//===--------------------------------------------------------------------===//

// TODO: These should be moved into a configuration file"
#define NVM_DIR "/mnt/pmfs/"
#define HDD_DIR "/data/"
#define SSD_DIR "/data1/"

#define TMP_DIR "/tmp/"

//===--------------------------------------------------------------------===//
// Storage Manager
//===--------------------------------------------------------------------===//

/// Stores data on different backends
class BackendManager {
 public:
  // global singleton
  static BackendManager &GetInstance(void);

  BackendManager();
  ~BackendManager();

  void *Allocate(BackendType type, size_t size);

  void Release(BackendType type, void *address);

  void Sync(BackendType type, void *address, size_t length);

  size_t GetMsyncCount() const { return msync_count; }

  size_t GetClflushCount() const { return clflush_count; }

  size_t GetAllocationCount() const { return allocation_count; }

 private:
  // data file address
  void *data_file_address;

  // data file lock
  common::synchronization::SpinLatch data_file_spinlock;

  // data file len
  size_t data_file_len;

  // data offset
  size_t data_file_offset;

  // stats
  size_t msync_count = 0;

  size_t clflush_count = 0;

  size_t allocation_count = 0;
};

}  // namespace storage
}  // namespace peloton
