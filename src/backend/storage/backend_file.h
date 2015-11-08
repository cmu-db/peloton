//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_backend.h
//
// Identification: src/backend/storage/backend_file.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/storage/backend.h"
#include <mutex>
#include <map>

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Backend (for physical storage)
//===--------------------------------------------------------------------===//

/// Represents a storage backend. Can reside on MM or NVM.
class BackendFile : public Backend {
 public:
  BackendFile();

  ~BackendFile();

  void *Allocate(size_t size);

  void Free(void *ptr);

  void Sync(void *ptr);

  // global singleton
  static Backend& GetInstance(void) {
    static BackendFile backend;
    return backend;
  }

  std::string GetBackendType() const {
    return BackendTypeToString(BACKEND_TYPE_FILE);
  }
 private:
  int fd = -1;
  char * backend_space = nullptr;
  size_t current_pos = 0;
  std::mutex backend_mutex;

  // Record chunks size for free and sync
  // XXX How to persist this information?
  std::map<void*, size_t> chunk_size_recs;

  std::string file_name = "backend.file";
  size_t file_size = 1024*1024*20;
};

}  // End storage namespace
}  // End peloton namespace
