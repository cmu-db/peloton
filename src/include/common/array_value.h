//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_value.h
//
// Identification: src/backend/common/array_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/value.h"

#include <vector>

//namespace peloton {
//namespace common {
//
//class ArrayValue : public Value {
// public:
//  template<class T>
//  ArrayValue(const std::vector<T> &vals, Type &element_type)
//                            : Value(Type::GetInstance(Type::ARRAY)),
//                              element_type_(element_type){
//    value_.ptr = (char *) &vals;
//  }
//  ~ArrayValue() {}
//
//  // Get the element at a given index in this array
//  Value *GetElementAt(uint64_t idx) const;
//
//  Type::TypeId GetElementType() const { return element_type_.GetTypeId(); }
//
//  // Does this value exist in this array?
//  Value *InList(const Value &o) const;
//
//  Value *CompareEquals(const Value &o) const override;
//  Value *CompareNotEquals(const Value &o) const override;
//  Value *CompareLessThan(const Value &o) const override;
//  Value *CompareLessThanEquals(const Value &o) const override;
//  Value *CompareGreaterThan(const Value &o) const override;
//  Value *CompareGreaterThanEquals(const Value &o) const override;
//
//  Value *CastAs(const Type::TypeId type_id) const override;
//
//  bool IsInlined() const override { return false; }
//  std::string ToString() const override { return ""; }
//  size_t Hash() const override { return 0; }
//  void HashCombine(size_t &seed UNUSED_ATTRIBUTE) const override {}
//
//  void SerializeTo(SerializeOutput &out UNUSED_ATTRIBUTE) const override {
//    throw Exception("Can't serialize array types to storage");
//  }
//
//  void SerializeTo(char *storage UNUSED_ATTRIBUTE,
//                   bool inlined UNUSED_ATTRIBUTE,
//                   VarlenPool *pool UNUSED_ATTRIBUTE) const override {
//    throw Exception("Can't serialize array types to storage");
//  }
//
//  Value *Copy() const override { return nullptr; }
//
// private:
//  Type &element_type_;
//};

}  // namespace peloton
}  // namespace common
