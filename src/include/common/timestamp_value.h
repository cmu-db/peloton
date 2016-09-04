//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_value.h
//
// Identification: src/backend/common/timestamp_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/value.h"

namespace peloton {
namespace common {

class TimestampValue : public Value {
 public:
  ~TimestampValue() {}
  TimestampValue(uint64_t val);
  
  // Comparison functions
  Value *CompareEquals(const Value &o) const override;
  Value *CompareNotEquals(const Value &o) const override;
  Value *CompareLessThan(const Value &o) const override;
  Value *CompareLessThanEquals(const Value &o) const override;
  Value *CompareGreaterThan(const Value &o) const override;
  Value *CompareGreaterThanEquals(const Value &o) const override;

  Value *CastAs(const Type::TypeId type_id) const override;

  bool IsInlined() const override { return true; }

  // Debug
  std::string ToString() const override;

  // Compute a hash value
  size_t Hash() const override;
  void HashCombine(size_t &seed) const override;

  void SerializeTo(SerializeOutput &out) const override;
  void SerializeTo(char *storage, bool inlined,
                   VarlenPool *pool) const override;

  // Create a copy of this value
  Value *Copy() const override;
};

}  // namespace peloton
}  // namespace common
