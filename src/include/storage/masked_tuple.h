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

#include <vector>
#include <sstream>

#include "catalog/schema.h"
#include "common/abstract_tuple.h"
#include "common/internal_types.h"
#include "type/value.h"

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
  inline MaskedTuple(AbstractTuple *rhs, const std::vector<oid_t> &mask)
      : tuple_(rhs), mask_(mask) {}

  ~MaskedTuple() {
      // We don't want to delete the AbstractTuple that we're pointing to
      // We don't own it. That's not on us!
  }

  inline void SetMask(const std::vector<oid_t> &mask) {
    // PL_ASSERT(mask_ == nullptr);
    mask_ = mask;
  }

  inline type::Value GetValue(oid_t column_id) const {
    return (tuple_->GetValue(mask_[column_id]));
  }

  inline void SetValue(oid_t column_id, const type::Value &value) {
    // Not sure if we want to support this...
    tuple_->SetValue(mask_[column_id], value);
  }

  inline char *GetData() const { return (tuple_->GetData()); }

  const std::string GetInfo() const {
    std::stringstream os;
    os << "**MaskedTuple** ";
    os << tuple_->GetInfo();
    return os.str();
  }

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // The real tuple that we are masking
  AbstractTuple *tuple_;

  // The length of this array has to be the same as the # of columns
  // in the underlying tuple schema.
  // MaskOffset -> RealOffset
  std::vector<oid_t> mask_;
};

//===--------------------------------------------------------------------===//
// Implementation
//===--------------------------------------------------------------------===//

}  // namespace storage
}  // namespace peloton
