//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type.h
//
// Identification: src/backend/type/type.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <string>
#include <functional>

#include "type/serializeio.h"
#include "type/type_id.h"
#include "common/internal_types.h"

namespace peloton {
namespace type {

class AbstractPool;
class Value;
class ValueFactory;

class Type {
 public:
  struct TypeIdHasher {
    size_t operator()(const TypeId &type_id) const {
      return std::hash<uint32_t>{}(static_cast<uint32_t>(type_id));
    }
  };

  Type(TypeId type_id) : type_id_(type_id) {}

  virtual ~Type() {}
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

  static Value GetMinValue(TypeId type_id);
  static Value GetMaxValue(TypeId type_id);

  inline static Type* GetInstance(TypeId type_id) { return kTypes[static_cast<int>(type_id)]; }

  inline TypeId GetTypeId() const { return type_id_; }

  // Comparison functions
  //
  // NOTE:
  // We could get away with only CompareLessThan() being purely virtual, since
  // the remaining comparison functions can derive their logic from
  // CompareLessThan(). For example:
  //
  //    CompareEquals(o) = !CompareLessThan(o) && !o.CompareLessThan(this)
  //    CompareNotEquals(o) = !CompareEquals(o)
  //    CompareLessThanEquals(o) = CompareLessThan(o) || CompareEquals(o)
  //    CompareGreaterThan(o) = !CompareLessThanEquals(o)
  //    ... etc. ...
  //
  // We don't do this for two reasons:
  // (1) The redundant calls to CompareLessThan() may be a performance problem,
  //     and since Value is a core component of the execution engine, we want to
  //     make it as performant as possible.
  // (2) Keep the interface consistent by making all functions purely virtual.
  virtual CmpBool CompareEquals(const Value& left, const Value& right) const;
  virtual CmpBool CompareNotEquals(const Value& left, const Value& right) const;
  virtual CmpBool CompareLessThan(const Value& left, const Value& right) const;
  virtual CmpBool CompareLessThanEquals(const Value& left,
                                        const Value& right) const;
  virtual CmpBool CompareGreaterThan(const Value& left,
                                     const Value& right) const;
  virtual CmpBool CompareGreaterThanEquals(const Value& left,
                                           const Value& right) const;

  // Other mathematical functions
  virtual Value Add(const Value& left, const Value& right) const;
  virtual Value Subtract(const Value& left, const Value& right) const;
  virtual Value Multiply(const Value& left, const Value& right) const;
  virtual Value Divide(const Value& left, const Value& right) const;
  virtual Value Modulo(const Value& left, const Value& right) const;
  virtual Value Min(const Value& left, const Value& right) const;
  virtual Value Max(const Value& left, const Value& right) const;
  virtual Value Sqrt(const Value& val) const;
  virtual Value OperateNull(const Value& val, const Value& right) const;
  virtual bool IsZero(const Value& val) const;

  // Is the data inlined into this classes storage space, or must it be accessed
  // through an indirection/pointer?
  virtual bool IsInlined(const Value& val) const;

  // Return a stringified version of this value
  virtual std::string ToString(const Value& val) const;

  // Compute a hash value
  virtual size_t Hash(const Value& val) const;
  virtual void HashCombine(const Value& val, size_t& seed) const;

  // Serialize this value into the given storage space. The inlined parameter
  // indicates whether we are allowed to inline this value into the storage
  // space, or whether we must store only a reference to this value. If inlined
  // is false, we may use the provided data pool to allocate space for this
  // value, storing a reference into the allocated pool space in the storage.
  virtual void SerializeTo(const Value& val, char* storage, bool inlined,
                           AbstractPool* pool) const;
  virtual void SerializeTo(const Value& val, SerializeOutput& out) const;

  // Deserialize a value of the given type from the given storage space.
  virtual Value DeserializeFrom(const char* storage, const bool inlined,
                                AbstractPool* pool = nullptr) const;
  virtual Value DeserializeFrom(SerializeInput& in,
                                AbstractPool* pool = nullptr) const;

  // Create a copy of this value
  virtual Value Copy(const Value& val) const;

  virtual Value CastAs(const Value& val, const TypeId type_id) const;

  // Access the raw variable length data
  virtual const char* GetData(const Value& val) const;

  // Get the length of the variable length data
  virtual uint32_t GetLength(const Value& val) const;

  // Access the raw varlen data stored from the tuple storage
  virtual char *GetData(char *storage);

  // Get the element at a given index in this array
  virtual Value GetElementAt(const Value& val, uint64_t idx) const;

  virtual TypeId GetElementType(const Value& val) const;

  // Does this value exist in this array?
  virtual Value InList(const Value& list, const Value& object) const;

 protected:
  // The actual type ID
  TypeId type_id_;

  // Singleton instances.
  static Type* kTypes[14];
};

}  // namespace type
}  // namespace peloton
