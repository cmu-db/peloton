//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// masked_tuple.h
//
// Identification: src/include/storage/masked_tuple.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "catalog/schema.h"
#include "common/abstract_tuple.h"
#include "type/serializeio.h"
#include "type/serializer.h"
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// MaskedTuple class
//===--------------------------------------------------------------------===//

/**
 * A 'MaskedTuple' is just a thin wrapper around a regular Tuple but it is
 * able to map columns to new offsets. This is to avoid having to make
 * copies of tuples just for index probes.
 */
class MaskedTuple : public AbstractTuple {
 public:
  inline MaskedTuple(const AbstractTuple *rhs) : tuple_(rhs), mask_(nullptr) {}

  ~MaskedTuple() {
    if (mask_ != nullptr) {
      delete mask_;
    }
  }

  inline void SetMask(int *mask) {
    PL_ASSERT(mask_ == nullptr);
    mask_ = mask;
  }

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // The real tuple that we are masking
  const AbstractTuple *tuple_;

  // The length of this array has to be the same as the # of columns
  // in the underlying tuple schema.
  // MaskOffset -> RealOffset
  int *mask_;
};

//===--------------------------------------------------------------------===//
// Implementation
//===--------------------------------------------------------------------===//

}  // End storage namespace
}  // End peloton namespace
