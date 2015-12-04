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
#include <string>

#include "backend/common/types.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Storage Manager
//===--------------------------------------------------------------------===//

/// Stores data on different backends
class StorageManager  {
 public:
  virtual ~StorageManager(){};

  // global singleton
  static StorageManager& GetInstance(void) {
    static StorageManager storage_manager;
    return storage_manager;
  }

  void *Allocate(size_t size, BackendType type);

  void Release(void *ptr, BackendType type);

  void Sync(void *ptr, BackendType type);

};

}  // End storage namespace
}  // End peloton namespace
