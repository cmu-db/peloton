//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_value.h
//
// Identification: src/backend/common/varlen_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/value.h"
//#include "common/varlen.h"

namespace peloton {
namespace common {

// A varlen value is an abstract class representing all objects that have
// variable length.
class VarlenValue : public Value {
 public:
  VarlenValue(const char *data, uint32_t len, bool binary);
  VarlenValue(const std::string &data, bool binary);
  ~VarlenValue();
  
  // Access the raw variable length data
  const char *GetData() const;

  // Get the length of the variable length data
  uint32_t GetLength() const;

  // Varchar string comparisons will be complicated
  Value *CompareEquals(const Value &o) const override;
  Value *CompareNotEquals(const Value &o) const override;
  Value *CompareLessThan(const Value &o) const override;
  Value *CompareLessThanEquals(const Value &o) const override;
  Value *CompareGreaterThan(const Value &o) const override;
  Value *CompareGreaterThanEquals(const Value &o) const override;

  Value *CastAs(const Type::TypeId type_id) const override;

  bool IsInlined() const override { return false; }

  // Debug
  std::string ToString() const override;

  // Compute a hash value
  size_t Hash() const override;
  void HashCombine(size_t &seed) const override;

  // Serialize this value into the given storage space
  void SerializeTo(SerializeOutput &out) const override;
  void SerializeTo(char *storage, bool inlined,
                   VarlenPool *pool) const override;

  // Create a copy of this value
  Value *Copy() const override;

 private:
  // If this val is not inlined, it may be beneficial to cache the length
  // into Value::buffer_ to avoid a secondary lookup. This field indicates
  // whether the length has been cached in the buffer. It should be set after
  // the first access.
  //bool cached_;
};

}  // namespace common
}  // namespace peloton
