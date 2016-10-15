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

  virtual ~Type();
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

  static Type * GetInstance(TypeId type_id);
  TypeId GetTypeId() const;

  Type(TypeId type_id) : type_id_(type_id) {}

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
  virtual Value *CompareEquals(const Value& left, const Value &right) const = 0;
  virtual Value *CompareNotEquals(const Value& left, const Value &right) const = 0;
  virtual Value *CompareLessThan(const Value& left, const Value &right) const = 0;
  virtual Value *CompareLessThanEquals(const Value& left, const Value &right) const = 0;
  virtual Value *CompareGreaterThan(const Value& left, const Value &right) const = 0;
  virtual Value *CompareGreaterThanEquals(const Value& left, const Value &right) const = 0;

  // Other mathematical functions
  virtual Value *Add(const Value& left, const Value &right) const = 0;
  virtual Value *Subtract(const Value& left, const Value &right) const = 0;
  virtual Value *Multiply(const Value& left, const Value &right) const = 0;
  virtual Value *Divide(const Value& left, const Value &right) const = 0;
  virtual Value *Modulo(const Value& left, const Value &right) const = 0;
  virtual Value *Min(const Value& left, const Value &right) const = 0;
  virtual Value *Max(const Value& left, const Value &right) const = 0;
  virtual Value *Sqrt(const Value& val) const = 0;
  virtual Value *OperateNull(const Value& val, const Value &right) const = 0;
  virtual bool IsZero(const Value& val) const = 0;

  // Is the data inlined into this classes storage space, or must it be accessed
  // through an indirection/pointer?
  virtual bool IsInlined(const Value& val) const = 0;

  // Return a stringified version of this value
  virtual std::string ToString(const Value& val) const = 0;

  // Compute a hash value
  virtual size_t Hash(const Value& val) const = 0;
  virtual void HashCombine(const Value& val, size_t &seed) const = 0;

  // Serialize this value into the given storage space. The inlined parameter
  // indicates whether we are allowed to inline this value into the storage
  // space, or whether we must store only a reference to this value. If inlined
  // is false, we may use the provided data pool to allocate space for this
  // value, storing a reference into the allocated pool space in the storage.
  virtual void SerializeTo(const Value& val, char *storage, bool inlined,
                           VarlenPool *pool) const = 0;
  virtual void SerializeTo(const Value& val, SerializeOutput &out) const = 0;

  // Create a copy of this value
  virtual Value *Copy(const Value& val) const = 0;

  virtual Value *CastAs(const Value& val, const Type::TypeId type_id) const = 0;

  // Access the raw variable length data
  virtual const char *GetData(const Value& val) const;

  // Get the length of the variable length data
  virtual uint32_t GetLength(const Value& val) const;

  // For unordered_map
  struct equal_to {
    bool operator()(const Value *x, const Value *y) const {
      std::unique_ptr<Value> cmp(x->CompareEquals(*y));
      return (cmp->IsTrue());
    }
  };

  template <class T>
  inline void hash_combine(std::size_t &seed, const T &v) const {
    std::hash<T> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }

  struct hash {
    size_t operator()(const Value *x) const {
      return x->Hash();
    }
  };

 private:
  // The actual type ID
  TypeId type_id_;

  // Singleton instances.
  static Type* kTypes[14];
};

}  // namespace common
}  // namespace peloton
