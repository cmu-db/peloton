//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// backend_nvm.h
//
// Identification: src/backend/storage/backend_nvm.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/storage/abstract_backend.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// NVM Backend (Non-volatile memory)
//===--------------------------------------------------------------------===//

class NVMBackend : public AbstractBackend {
 public:
  virtual ~NVMBackend(){};

  void *Allocate(size_t size) { return ::operator new(size); }

  void Free(void *ptr) { ::operator delete(ptr); }

  void Sync(void *ptr __attribute__((unused))) {
    // does nothing
  }

  std::string GetBackendType() const {
    return BackendTypeToString(BACKEND_TYPE_NVM);
  }
};

}  // End storage namespace
}  // End peloton namespace
