//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type.h
//
// Identification: src/backend/common/type.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <string>

namespace peloton {
namespace common {

class Value;
class NumericValue;
class IntegerValue;

class Type {
 public:
  enum TypeId {
    INVALID,
    PARAMETER_OFFSET,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    DECIMAL,
    TIMESTAMP,
    DATE,
    VARCHAR,
    VARBINARY,
    ARRAY,
    UDT
};

  Type() {}

  // Is this type equivalent to the other
  bool Equals(const Type &other) const;

  // Get the size of this data type in bytes
  static uint64_t GetTypeSize(TypeId type_id);

  // What is the resulting type of performing the templated binary operation
  // on the templated types
  template <typename Op, typename T1, typename T2>
  static Type GetResultOfBinaryOp();
  
  // Is this type coercable from the other type
  bool IsCoercableFrom(const TypeId type_id) const;

  // Debug
  std::string ToString() const;

  static Value *GetMinValue(TypeId type_id);
  static Value *GetMaxValue(TypeId type_id);

  static Type &GetInstance(TypeId type_id);
  TypeId GetTypeId() const;

  Type(TypeId type_id) : type_id_(type_id) {}

 private:
  // The actual type ID
  TypeId type_id_;

  // Singleton instances.
  static Type kTypes[14];
};

}  // namespace common
}  // namespace peloton