//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen.h
//
// Identification: src/backend/common/varlen.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include "common/varlen_pool.h"

namespace peloton {
namespace common {

class VarlenPool;

// Represents a variable-length opaque collection of bytes.
class Varlen {
 public:
  // Create and return a new Varlen object of the given size. The caller may
  // optionally provide a pool from which memory can be requested to allocate
  // an object. If no pool is allocated, the implementation is free to acquire
  // memory from anywhere she pleases, including a thread local pool or the
  // global heap memory space.
  Varlen(size_t size, VarlenPool *pool = nullptr);

  // Destroy this object, freeing and returning any memory that was allocated
  // at the time of construction.
  ~Varlen();

  // Clone this Varlen object, acquiring memory from the (optionall) provided
  // varlen pool.
  Varlen *Clone(const Varlen &src, VarlenPool *pool = nullptr) const;

  // Get access to the raw bytes this object refers to
  char *GetRaw() { return data_ptr_; }
  const char *GetRaw() const { return data_ptr_; }
  size_t  GetSize() const { return data_size_; }

 private:
  // The pointer to the memory space where the data is stored
  char *data_ptr_;
  size_t data_size_;
  // Indicates whether this varlen object owns the memory space, or whether it
  // was acquired from an external memory pool
  bool owns_data_;
  // If this varlen doesn't own the data, it belongs to this varlen pool and
  // must be returned when destroyed.
  VarlenPool *pool_;
};

}  // namespace common
}  // namespace peloton