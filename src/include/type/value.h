//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value.h
//
// Identification: src/backend/type/value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>

#include "common/exception.h"
#include "common/macros.h"
#include "common/printable.h"

#include "type/limits.h"
#include "type/serializeio.h"
#include "type/type.h"

#include "util/string_util.h"

namespace peloton {
namespace type {

class Type;

inline CmpBool GetCmpBool(bool boolean) {
  return boolean ? CmpBool::TRUE : CmpBool::FALSE;
}

// A value is an abstract class that represents a view over SQL data stored in
// some materialized state. All values have a type and comparison functions, but
// subclasses implement other type-specific functionality.
class Value : public Printable {
#ifdef VALUE_TESTS
 public:
#else
 private:
#endif

  Value(const TypeId type) : manage_data_(false), type_id_(type) {
    size_.len = PELOTON_VALUE_NULL;
  }

  // ARRAY values
  template <class T>
  Value(TypeId type, const std::vector<T> &vals, TypeId element_type);

  // BOOLEAN and TINYINT
  Value(TypeId type, int8_t val);

  // DECIMAL
  Value(TypeId type, double d);
  Value(TypeId type, float f);

  // SMALLINT
  Value(TypeId type, int16_t i);
  // INTEGER and PARAMETER_OFFSET and DATE
  Value(TypeId type, int32_t i);
  // BIGINT
  Value(TypeId type, int64_t i);

  // TIMESTAMP
  Value(TypeId type, uint64_t i);

  // VARCHAR and VARBINARY
  Value(TypeId type, const char *data, uint32_t len, bool manage_data);
  Value(TypeId type, const std::string &data);

 public:
  Value();
  Value(Value &&other);
  Value(const Value &other);
  ~Value();

  friend void swap(Value &first, Value &second)  // nothrow
  {
    std::swap(first.value_, second.value_);
    std::swap(first.size_, second.size_);
    std::swap(first.manage_data_, second.manage_data_);
    std::swap(first.type_id_, second.type_id_);
  }

  Value &operator=(Value other);

  // Get the type of this value
  inline TypeId GetTypeId() const { return type_id_; }
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
  inline CmpBool CompareEquals(const Value &o) const {
    return Type::GetInstance(type_id_)->CompareEquals(*this, o);
  }
  inline CmpBool CompareNotEquals(const Value &o) const {
    return Type::GetInstance(type_id_)->CompareNotEquals(*this, o);
  }
  inline CmpBool CompareLessThan(const Value &o) const {
    return Type::GetInstance(type_id_)->CompareLessThan(*this, o);
  }
  inline CmpBool CompareLessThanEquals(const Value &o) const {
    return Type::GetInstance(type_id_)->CompareLessThanEquals(*this, o);
  }
  inline CmpBool CompareGreaterThan(const Value &o) const {
    return Type::GetInstance(type_id_)->CompareGreaterThan(*this, o);
  }
  inline CmpBool CompareGreaterThanEquals(const Value &o) const {
    return Type::GetInstance(type_id_)->CompareGreaterThanEquals(*this, o);
  }

  // Other mathematical functions
  inline Value Add(const Value &o) const {
    return Type::GetInstance(type_id_)->Add(*this, o);
  }
  inline Value Subtract(const Value &o) const {
    return Type::GetInstance(type_id_)->Subtract(*this, o);
  }
  inline Value Multiply(const Value &o) const {
    return Type::GetInstance(type_id_)->Multiply(*this, o);
  }
  inline Value Divide(const Value &o) const {
    return Type::GetInstance(type_id_)->Divide(*this, o);
  }
  inline Value Modulo(const Value &o) const {
    return Type::GetInstance(type_id_)->Modulo(*this, o);
  }
  inline Value Min(const Value &o) const {
    return Type::GetInstance(type_id_)->Min(*this, o);
  }
  inline Value Max(const Value &o) const {
    return Type::GetInstance(type_id_)->Max(*this, o);
  }
  inline Value Sqrt() const { return Type::GetInstance(type_id_)->Sqrt(*this); }
  inline Value OperateNull(const Value &o) const {
    return Type::GetInstance(type_id_)->OperateNull(*this, o);
  }
  inline bool IsZero() const {
    return Type::GetInstance(type_id_)->IsZero(*this);
  }

  // Is the data inlined into this classes storage space, or must it be accessed
  // through an indirection/pointer?
  inline bool IsInlined() const {
    return Type::GetInstance(type_id_)->IsInlined(*this);
  }

  // Is a value null?
  inline bool IsNull() const { return size_.len == PELOTON_VALUE_NULL; }

  // Examine the type of this object.
  bool CheckInteger() const;

  // Can two types of value be compared?
  bool CheckComparable(const Value &o) const;

  inline bool IsTrue() const {
    PL_ASSERT(GetTypeId() == TypeId::BOOLEAN);
    return (value_.boolean == 1);
  }

  inline bool IsFalse() const {
    PL_ASSERT(GetTypeId() == TypeId::BOOLEAN);
    return (value_.boolean == 0);
  }

  // Return a stringified version of this value
  inline std::string ToString() const {
    return Type::GetInstance(type_id_)->ToString(*this);
  }

  // Compute a hash value
  inline size_t Hash() const {
    return Type::GetInstance(type_id_)->Hash(*this);
  }

  inline void HashCombine(size_t &seed) const {
    return Type::GetInstance(type_id_)->HashCombine(*this, seed);
  }

  // Serialize this value into the given storage space. The inlined parameter
  // indicates whether we are allowed to inline this value into the storage
  // space, or whether we must store only a reference to this value. If inlined
  // is false, we may use the provided data pool to allocate space for this
  // value, storing a reference into the allocated pool space in the storage.
  inline void SerializeTo(char *storage, bool inlined,
                          AbstractPool *pool) const {
    Type::GetInstance(type_id_)->SerializeTo(*this, storage, inlined, pool);
  }

  inline void SerializeTo(SerializeOutput &out) const {
    Type::GetInstance(type_id_)->SerializeTo(*this, out);
  }

  // Deserialize a value of the given type from the given storage space.
  inline static Value DeserializeFrom(const char *storage, const TypeId type_id,
                                      const bool inlined,
                                      AbstractPool *pool = nullptr) {
    return Type::GetInstance(type_id)->DeserializeFrom(storage, inlined, pool);
  }

  inline static Value DeserializeFrom(SerializeInput &in, const TypeId type_id,
                                      AbstractPool *pool = nullptr) {
    return Type::GetInstance(type_id)->DeserializeFrom(in, pool);
  }

  // Access the raw variable length data
  inline const char *GetData() const {
    return Type::GetInstance(type_id_)->GetData(*this);
  }

  // Access the raw variable length data from a pointer pointed to a tuple
  // storage
  inline static char *GetDataFromStorage(TypeId type_id, char *storage) {
    switch (type_id) {
      case TypeId::VARCHAR:
      case TypeId::VARBINARY: {
        return Type::GetInstance(type_id)->GetData(storage);
      }
      default:
        throw Exception(ExceptionType::INCOMPATIBLE_TYPE,
                        "Invalid Type for getting raw data pointer");
    }
  }

  // Get the length of the variable length data
  inline uint32_t GetLength() const {
    return Type::GetInstance(type_id_)->GetLength(*this);
  }

  template <class T>
  inline T GetAs() const {
    return *reinterpret_cast<const T *>(&value_);
  }

  // Create a copy of this value
  inline Value Copy() const { return Type::GetInstance(type_id_)->Copy(*this); }

  inline Value CastAs(const TypeId type_id) const {
    return Type::GetInstance(type_id_)->CastAs(*this, type_id);
  }

  // Get the element at a given index in this array
  inline Value GetElementAt(uint64_t idx) const {
    return Type::GetInstance(type_id_)->GetElementAt(*this, idx);
  }

  inline TypeId GetElementType() const {
    return Type::GetInstance(type_id_)->GetElementType(*this);
  }

  // Does this value exist in this array?
  inline Value InList(const Value &object) const {
    return Type::GetInstance(type_id_)->InList(*this, object);
  }

  // For unordered_map
  struct equal_to {
    inline bool operator()(const Value &x, const Value &y) const {
      return Type::GetInstance(x.type_id_)->CompareEquals(x, y) == CmpBool::TRUE;
    }
  };

  template <class T>
  inline void hash_combine(std::size_t &seed, const T &v) const {
    std::hash<T> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }

  struct hash {
    size_t operator()(const Value &x) const {
      return Type::GetInstance(x.type_id_)->Hash(x);
    }
  };

  friend struct equal_to;
  friend struct hash_combine;
  friend struct hash;

  // Type classes
  friend class Type;
  friend class ArrayType;
  friend class BooleanType;
  friend class NumericType;
  friend class IntegerParentType;
  friend class TinyintType;
  friend class SmallintType;
  friend class IntegerType;
  friend class BigintType;
  friend class DecimalType;
  friend class VarlenType;
  friend class TimestampType;
  friend class DateType;

  friend class ValueFactory;

 protected:
  // The actual value item
  union Val {
    int8_t boolean;
    int8_t tinyint;
    int16_t smallint;
    int32_t integer;
    int64_t bigint;
    double decimal;
    int32_t date;
    uint64_t timestamp;
    char *varlen;
    const char *const_varlen;
    char *array;
  } value_;

  union {
    uint32_t len;
    TypeId elem_type_id;
  } size_;

  bool manage_data_;
  // TODO: Pack allocated flag with the type id
  // The data type
  TypeId type_id_;
};

// ARRAY here to ease creation of templates
// TODO: Fix the representation for a null array
template <class T>
Value::Value(TypeId type, const std::vector<T> &vals, TypeId element_type)
    : Value(TypeId::ARRAY) {
  switch (type) {
    case TypeId::ARRAY:
      value_.array = (char *)&vals;
      size_.elem_type_id = element_type;
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%d' for Array Value constructor",
                             static_cast<int>(type));
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

}  // namespace type
}  // namespace peloton
