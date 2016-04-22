//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// storage_manager.h
//
// Identification: src/backend/storage/storage_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <mutex>

#include "backend/common/types.h"
#include "backend/common/platform.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Storage Manager
//===--------------------------------------------------------------------===//

/// Stores data on different backends
class StorageManager {
 public:
  // global singleton
  static StorageManager &GetInstance(void);

  StorageManager();
  ~StorageManager();

  void *Allocate(BackendType type, size_t size);

  void Release(BackendType type, void *address);

  void Sync(BackendType type, void *address, size_t length);

  size_t GetMsyncCount() const {
    return msync_count;
  }

  size_t GetClflushCount() const {
    return clflush_count;
  }

  size_t GetAllocationCount() const {
    return allocation_count;
  }

 private:
  // data file address
  void *data_file_address;

  // data file lock
  Spinlock data_file_spinlock;

  // data file len
  size_t data_file_len;

  // data offset
  size_t data_file_offset;

  // stats
  size_t msync_count = 0;

  size_t clflush_count = 0;

  size_t allocation_count = 0;
};

}  // End storage namespace
}  // End peloton namespace
