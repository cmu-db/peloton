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
#include <libvmem.h>
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
  ~BackendFile();

  void *Allocate(size_t size);

  void Free(void *ptr);

  void Sync(void *ptr);

  // global singleton
  static BackendFile& GetInstance(void) {
    static BackendFile backend;
    return backend;
  }

  std::string GetBackendType() const {
    return BackendTypeToString(BACKEND_TYPE_FILE);
  }

  bool EnableBackFileType() {
    char* filebackend_str = getenv("ENABLE_FILEBACKEND");
    if (filebackend_str) {
      return atoi(filebackend_str) == 1;
    } else {
      return false;
    }
  }

 private:
  BackendFile();

  int fd = -1;
  void * backend_space = nullptr;
  VMEM *vmp = nullptr;

  std::string file_name = "/tmp/backend.file";
  size_t file_size = 1024*1024*1000;
};

}  // End storage namespace
}  // End peloton namespace
