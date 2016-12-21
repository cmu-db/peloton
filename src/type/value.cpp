//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value.h
//
// Identification: src/backend/type/value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/type/value.h>
#include "type/value.h"

#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/numeric_type.h"
#include "type/timestamp_type.h"
#include "type/varlen_type.h"

namespace peloton {
namespace type {

Value::Value(const Value &other) {
  type_ = other.type_;
  size_ = other.size_;
  switch (type_->GetTypeId()) {
    case Type::VARCHAR:
    case Type::VARBINARY:
      if (size_.len == PELOTON_VALUE_NULL) {
        value_.varlen = nullptr;
      } else {
        value_.varlen = new char[size_.len];
        PL_MEMCPY(value_.varlen, other.value_.varlen,
                  size_.len);
      }
      break;
    default:
      value_ = other.value_;
  }
}

Value::Value(Value &&other) : Value() { swap(*this, other); }

Value &Value::operator=(Value other) {
  swap(*this, other);
  return *this;
}

// ARRAY is implemented in the header to ease template creation

// BOOLEAN and TINYINT
Value::Value(Type::TypeId type, int8_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len = (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TINYINT:
      value_.tinyint = i;
      size_.len = (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::SMALLINT:
      value_.smallint = i;
      size_.len = (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len = (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::BIGINT:
      value_.bigint = i;
      size_.len = (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default:
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                      "Invalid Type for constructor");
  }
}

// SMALLINT
Value::Value(Type::TypeId type, int16_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len = (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TINYINT:
      value_.tinyint = i;
      size_.len = (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::SMALLINT:
      value_.smallint = i;
      size_.len = (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len = (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::BIGINT:
      value_.bigint = i;
      size_.len = (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default:
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                      "Invalid Type for constructor");
  }
}

// INTEGER and PARAMETER_OFFSET
Value::Value(Type::TypeId type, int32_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len = (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TINYINT:
      value_.tinyint = i;
      size_.len = (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::SMALLINT:
      value_.smallint = i;
      size_.len = (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len = (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::BIGINT:
      value_.bigint = i;
      size_.len = (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default:
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                      "Invalid Type for constructor");
  }
}

// BIGINT and TIMESTAMP
Value::Value(Type::TypeId type, int64_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len = (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TINYINT:
      value_.tinyint = i;
      size_.len = (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::SMALLINT:
      value_.smallint = i;
      size_.len = (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len = (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::BIGINT:
      value_.bigint = i;
      size_.len = (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TIMESTAMP:
      value_.timestamp = i;
      size_.len = (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default:
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                      "Invalid Type for constructor");
  }
}

// BIGINT
Value::Value(Type::TypeId type, uint64_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len = (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TIMESTAMP:
      value_.timestamp = i;
      size_.len = (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default:
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                      "Invalid Type for constructor");
  }
}

// DECIMAL
Value::Value(Type::TypeId type, double d) : Value(type) {
  switch (type) {
    case Type::DECIMAL:
      value_.decimal = d;
      size_.len = (value_.decimal == PELOTON_DECIMAL_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default:
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                      "Invalid Type for constructor");
  }
}

Value::Value(Type::TypeId type, float f) : Value(type) {
  switch (type) {
    case Type::DECIMAL:
      value_.decimal = f;
      size_.len = (value_.decimal == PELOTON_DECIMAL_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default:
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                      "Invalid Type for constructor");
  }
}

// VARCHAR and VARBINARY
Value::Value(Type::TypeId type, const char *data, uint32_t len) : Value(type) {
  switch (type) {
    case Type::VARCHAR:
    case Type::VARBINARY:
      if (data == nullptr) {
        value_.varlen = nullptr;
        size_.len = PELOTON_VALUE_NULL;
      } else {
        PL_ASSERT(len < PELOTON_VARCHAR_MAX_LEN);
        value_.varlen = new char[len];
        PL_ASSERT(value_.varlen != nullptr);
        size_.len = len;
        PL_MEMCPY(value_.varlen, data, len);
      }
      break;
    default:
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                      "Invalid Type for constructor");
  }
}

Value::Value(Type::TypeId type, const std::string &data) : Value(type) {
  switch (type) {
    case Type::VARCHAR:
    case Type::VARBINARY: {
      // TODO: How to represent a null string here?
      uint32_t len = data.length() + (type == Type::VARCHAR);
      value_.varlen = new char[len];
      PL_ASSERT(value_.varlen != nullptr);
      size_.len = len;
      PL_MEMCPY(value_.varlen, data.c_str(), len);
      break;
    }
    default:
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                      "Invalid Type for constructor");
  }
}

Value::Value() : Value(Type::INVALID) {}

Value::~Value() {
  switch (type_->GetTypeId()) {
    case Type::VARBINARY:
    case Type::VARCHAR:
      delete[] value_.varlen;
      break;
    default:
      break;
  }
}

const std::string Value::GetInfo() const {
  std::ostringstream os;
  os << "Value::" << Type::GetInstance(GetTypeId())->ToString() << "["
     << ToString() << "]";
  return os.str();
}

bool Value::CheckComparable(const Value &o) const {
  switch (GetTypeId()) {
    case Type::BOOLEAN:
      if (o.GetTypeId() == Type::BOOLEAN) return true;
      break;
    case Type::TINYINT:
    case Type::SMALLINT:
    case Type::INTEGER:
    case Type::BIGINT:
    case Type::DECIMAL:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
        case Type::SMALLINT:
        case Type::INTEGER:
        case Type::BIGINT:
        case Type::DECIMAL:
          return true;
        default:
          break;
      }
      break;
    case Type::VARCHAR:
      if (o.GetTypeId() == Type::VARCHAR) return true;
      break;
    case Type::VARBINARY:
      if (o.GetTypeId() == Type::VARBINARY) return true;
      break;
    case Type::TIMESTAMP:
      if (o.GetTypeId() == Type::TIMESTAMP) return true;
      break;
    default:
      break;
  }
  return false;
}

bool Value::CheckInteger() const {
  switch (GetTypeId()) {
    case Type::TINYINT:
    case Type::SMALLINT:
    case Type::INTEGER:
    case Type::BIGINT:
    case Type::PARAMETER_OFFSET:
      return true;
    default:
      break;
  }

  return false;
}

// Is the data inlined into this classes storage space, or must it be accessed
// through an indirection/pointer?
bool Value::IsInlined() const { return type_->IsInlined(*this); }

}  // namespace peloton
}  // namespace type
