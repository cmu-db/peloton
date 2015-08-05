//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_backend.h
//
// Identification: src/backend/storage/abstract_backend.h
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
class AbstractBackend {
 public:
  virtual ~AbstractBackend(){};

  //===--------------------------------------------------------------------===//
  // Interface
  //===--------------------------------------------------------------------===//

  virtual void *Allocate(size_t size) = 0;

  virtual void Free(void *ptr) = 0;

  virtual void Sync(void *ptr) = 0;

  virtual std::string GetBackendType() const = 0;
};

}  // End storage namespace
}  // End peloton namespace
