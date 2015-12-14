//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// storage_manager.cpp
//
// Identification: src/backend/storage/storage_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "backend/storage/storage_manager.h"

namespace peloton {
namespace storage {

void *StorageManager::Allocate(size_t size, BackendType type __attribute__((unused))) {
  return ::operator new(size);
}

void StorageManager::Release(void *ptr, BackendType type __attribute__((unused))) {
  ::operator delete(ptr);
}

void StorageManager::Sync(void *ptr __attribute__((unused)), BackendType type __attribute__((unused))) {
  // does nothing
}

}  // End storage namespace
}  // End peloton namespace
