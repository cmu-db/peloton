//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// backend_vm.h
//
// Identification: src/backend/storage/backend_vm.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/storage/abstract_backend.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// VM Backend
//===--------------------------------------------------------------------===//

class VMBackend : public AbstractBackend {
 public:
  virtual ~VMBackend(){};

  // global singleton
  static VMBackend& GetInstance(void) {
    static VMBackend vm_backend;
    return vm_backend;
  }

  void *Allocate(size_t size) { return ::operator new(size); }

  void Free(void *ptr) { ::operator delete(ptr); }

  void Sync(void *ptr __attribute__((unused))) {
    // does nothing
  }

  std::string GetBackendType() const {
    return BackendTypeToString(BACKEND_TYPE_VM);
  }
};

}  // End storage namespace
}  // End peloton namespace
