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

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Filesystem directories
//===--------------------------------------------------------------------===//

#define NVM_DIR "/mnt/pmfs/"
#define HDD_DIR "/data/"
#define SSD_DIR "/data1/"

#define TMP_DIR "/tmp/"

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

 private:
  // pmem file address
  char *data_file_address;

  // pmem file synch mutex
  std::mutex pmem_mutex;

  // is it actually pmem ?
  int is_pmem;

  // pmem file len
  size_t data_file_len;

  // pmem offset
  size_t data_file_offset;
};

}  // End storage namespace
}  // End peloton namespace
