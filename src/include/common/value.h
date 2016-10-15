//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value.h
//
// Identification: src/backend/common/value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/type.h"
#include "common/varlen_pool.h"
#include "serializeio.h"
#include "common/serializer.h"
#include "printable.h"
#include <climits>
#include <cfloat>
#include <cstdint>
#include "common/macros.h"

namespace peloton {
namespace common {

static const double DBL_LOWEST = std::numeric_limits<double>::lowest();
static const double FLT_LOWEST = std::numeric_limits<float>::lowest();

static const int8_t PELOTON_INT8_MIN = (SCHAR_MIN + 1);
static const int16_t PELOTON_INT16_MIN = (SHRT_MIN + 1);
static const int32_t PELOTON_INT32_MIN = (INT_MIN + 1);
static const int64_t PELOTON_INT64_MIN = (LLONG_MIN + 1);
static const double PELOTON_DECIMAL_MIN = FLT_LOWEST;
static const uint64_t PELOTON_TIMESTAMP_MIN = 0;
static const int8_t PELOTON_BOOLEAN_MIN = 0;

static const int8_t PELOTON_INT8_MAX = SCHAR_MAX;
static const int16_t PELOTON_INT16_MAX = SHRT_MAX;
static const int32_t PELOTON_INT32_MAX = INT_MAX;
static const int64_t PELOTON_INT64_MAX = LLONG_MAX;
static const uint64_t PELOTON_UINT64_MAX = ULLONG_MAX - 1;
static const double PELOTON_DECIMAL_MAX = DBL_MAX;
static const uint64_t PELOTON_TIMESTAMP_MAX = 11231999986399999999U;
static const int8_t PELOTON_BOOLEAN_MAX = 1;

static const int8_t PELOTON_INT8_NULL = SCHAR_MIN;
static const int16_t PELOTON_INT16_NULL = SHRT_MIN;
static const int32_t PELOTON_INT32_NULL = INT_MIN;
static const int64_t PELOTON_INT64_NULL = LLONG_MIN;
static const uint64_t PELOTON_TIMESTAMP_NULL = ULLONG_MAX;
static const double PELOTON_DECIMAL_NULL = DBL_LOWEST;
static const int8_t PELOTON_BOOLEAN_NULL = SCHAR_MIN;

static const uint32_t PELOTON_VARCHAR_MAX_LEN = UINT_MAX;

// A value is an abstract class that represents a view over SQL data stored in
// some materialized state. All values have a type and comparison functions, but
// subclasses implement other type-specific functionality.
class Value : public Printable {
 public:
  Value(const Type::TypeId type) : type_(Type::GetInstance(type)) {}

  //ARRAY values
  //TODO: implement array values

  //BOOLEAN and TINYINT
  Value(Type::TypeId type, int8_t val);

  //DECIMAL
  Value(Type::TypeId type, double d);
  Value(Type::TypeId type, float f);

  //SMALLINT
  Value(Type::TypeId type, int16_t i);
  //INTEGER and PARAMETER_OFFSET
  Value(Type::TypeId type, int32_t i);
  //BIGINT
  Value(Type::TypeId type, int64_t i);

  //TIMESTAMP
  Value(Type::TypeId type, uint64_t i);

  //VARCHAR and VARBINARY
  Value(Type::TypeId type, const char *data, uint32_t len);
  Value(Type::TypeId type, const std::string &data) ;


  ~Value() = 0;

  // Get the type of this value
  Type::TypeId GetTypeId() const { return type_->GetTypeId(); }
  const std::string GetInfo() const override;
  
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
  Value *CompareEquals(const Value &o) const = 0;
  Value *CompareNotEquals(const Value &o) const = 0;
  Value *CompareLessThan(const Value &o) const = 0;
  Value *CompareLessThanEquals(const Value &o) const = 0;
  Value *CompareGreaterThan(const Value &o) const = 0;
  Value *CompareGreaterThanEquals(const Value &o) const = 0;

  // Other mathematical functions
  Value *Add(const Value &o) const = 0;
  Value *Subtract(const Value &o) const = 0;
  Value *Multiply(const Value &o) const = 0;
  Value *Divide(const Value &o) const = 0;
  Value *Modulo(const Value &o) const = 0;
  Value *Min(const Value &o) const = 0;
  Value *Max(const Value &o) const = 0;
  Value *Sqrt() const = 0;
  Value *OperateNull(const Value &o) const = 0;
  bool IsZero() const = 0;

  // Is the data inlined into this classes storage space, or must it be accessed
  // through an indirection/pointer?
  bool IsInlined() const = 0;

  // Is a value null?
  bool IsNull() const;

  // Examine the type of this object.
  void CheckInteger() const;

  // Can two types of value be compared?
  void CheckComparable(const Value &o) const;

  bool IsTrue() const {
    PL_ASSERT(GetTypeId() == Type::BOOLEAN);
    return (value_.boolean == 1);
  }

  bool IsFalse() const {
    PL_ASSERT(GetTypeId() == Type::BOOLEAN);
    return (value_.boolean == 0);
  }

  // Return a stringified version of this value
  std::string ToString() const = 0;

  // Compute a hash value
  size_t Hash() const = 0;
  void HashCombine(size_t &seed) const = 0;

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

  // Serialize this value into the given storage space. The inlined parameter
  // indicates whether we are allowed to inline this value into the storage
  // space, or whether we must store only a reference to this value. If inlined
  // is false, we may use the provided data pool to allocate space for this
  // value, storing a reference into the allocated pool space in the storage.
  void SerializeTo(char *storage, bool inlined,
                           VarlenPool *pool) const = 0;
  void SerializeTo(SerializeOutput &out) const = 0;

  // Deserialize a value of the given type from the given storage space.
  static Value *DeserializeFrom(const char *storage, const Type::TypeId type_id, 
                                const bool inlined, VarlenPool *pool = nullptr);
  static Value *DeserializeFrom(SerializeInput &in, const Type::TypeId type_id,
                                VarlenPool *pool = nullptr);

  // Access the raw variable length data
  const char *GetData(const Value& val) const;

  // Get the length of the variable length data
  uint32_t GetLength(const Value& val) const;

  template <class T>
  T GetAs() const { return *reinterpret_cast<const T*>(&value_); }

  // Create a copy of this value
  Value *Copy() const = 0;

  Value *CastAs(const Type::TypeId type_id) const = 0;

 protected:
  // The data type
  Type *type_;

  // The actual value item
  union Val {
    int8_t boolean;
    int8_t tinyint;
    int16_t smallint;
    int32_t integer;
    int64_t bigint;
    double decimal;
    uint64_t timestamp;
    char *ptr;
  } value_;
};

}  // namespace common
}  // namespace peloton
