//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_value.h
//
// Identification: src/backend/type/varlen_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/value.h"

namespace peloton {
namespace type {

// A varlen value is an abstract class representing all objects that have
// variable length.
class VarlenType : public Type {
 public:
  VarlenType(TypeId type);
  ~VarlenType();
  
  // Access the raw variable length data
  const char *GetData(const Value& val) const override;

  // Access the raw varlen data stored from the tuple storage
  char *GetData(char *storage) override;

  // Get the length of the variable length data
  uint32_t GetLength(const Value& val) const override;

  // Comparison functions
  CmpBool CompareEquals(const Value& left, const Value &right) const override;
  CmpBool CompareNotEquals(const Value& left, const Value &right) const override;
  CmpBool CompareLessThan(const Value& left, const Value &right) const override;
  CmpBool CompareLessThanEquals(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThan(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThanEquals(const Value& left, const Value &right) const override;
  
  // Other mathematical functions
  virtual Value Min(const Value& left, const Value &right) const override;
  virtual Value Max(const Value& left, const Value &right) const override;

  Value CastAs(const Value& val, const TypeId type_id) const override;

  // Decimal types are always inlined
  bool IsInlined(const Value&) const override { return false; }

  // Debug
  std::string ToString(const Value& val) const override;

  // Compute a hash value
  size_t Hash(const Value& val) const override;
  void HashCombine(const Value& val, size_t &seed) const override;

  // Serialize this value into the given storage space
  void SerializeTo(const Value& val, SerializeOutput &out) const override;
  void SerializeTo(const Value& val, char *storage, bool inlined,
                   AbstractPool *pool) const override;

  // Deserialize a value of the given type from the given storage space.
  Value DeserializeFrom(const char *storage,
                                const bool inlined, AbstractPool *pool = nullptr) const override;
  Value DeserializeFrom(SerializeInput &in,
                                AbstractPool *pool = nullptr) const override;

  // Create a copy of this value
  Value Copy(const Value& val) const override;
};

}  // namespace type
}  // namespace peloton
