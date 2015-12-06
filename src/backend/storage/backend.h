//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_backend.h
//
// Identification: src/backend/storage/backend.h
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
// Backend (for physical storage)
//===--------------------------------------------------------------------===//

/// Represents a storage backend. Can reside on MM or NVM.
class Backend  {
 public:
  virtual ~Backend(){};

  // global singleton
  static Backend& GetInstance(void) {
    static Backend backend;
    return backend;
  }

  virtual void *Allocate(size_t size) { return ::operator new(size); }

  virtual void Free(void *ptr) { ::operator delete(ptr); }

  virtual void Sync(void *ptr __attribute__((unused))) {
    // does nothing
  }

  virtual std::string GetBackendType() const {
    return BackendTypeToString(BACKEND_TYPE_VM);
  }
};

}  // End storage namespace
}  // End peloton namespace
