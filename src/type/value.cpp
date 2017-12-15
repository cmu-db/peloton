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

#include "type/value.h"

#include <sstream>

#include "common/logger.h"
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/numeric_type.h"
#include "type/timestamp_type.h"
#include "type/varlen_type.h"

namespace peloton {
namespace type {

Value::Value(const Value &other) {
  type_id_ = other.type_id_;
  size_ = other.size_;
  manage_data_ = other.manage_data_;
  switch (type_id_) {
    case TypeId::VARCHAR:
    case TypeId::VARBINARY:
      if (size_.len == PELOTON_VALUE_NULL) {
        value_.varlen = nullptr;
      } else {
        if (manage_data_) {
          value_.varlen = new char[size_.len];
          PL_MEMCPY(value_.varlen, other.value_.varlen, size_.len);
        } else {
          value_ = other.value_;
        }
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
Value::Value(TypeId type, int8_t i) : Value(type) {
  switch (type) {
    case TypeId::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::TINYINT:
      value_.tinyint = i;
      size_.len =
          (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::SMALLINT:
      value_.smallint = i;
      size_.len =
          (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::INTEGER:
    case TypeId::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len =
          (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::BIGINT:
      value_.bigint = i;
      size_.len =
          (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for 1-byte Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

// SMALLINT
Value::Value(TypeId type, int16_t i) : Value(type) {
  switch (type) {
    case TypeId::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::TINYINT:
      value_.tinyint = i;
      size_.len =
          (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::SMALLINT:
      value_.smallint = i;
      size_.len =
          (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::INTEGER:
    case TypeId::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len =
          (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::BIGINT:
      value_.bigint = i;
      size_.len =
          (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for 2-byte Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

// INTEGER and PARAMETER_OFFSET
Value::Value(TypeId type, int32_t i) : Value(type) {
  switch (type) {
    case TypeId::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::TINYINT:
      value_.tinyint = i;
      size_.len =
          (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::SMALLINT:
      value_.smallint = i;
      size_.len =
          (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::INTEGER:
    case TypeId::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len =
          (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::BIGINT:
      value_.bigint = i;
      size_.len =
          (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::DATE:
      value_.date = i;
      size_.len = (value_.date == PELOTON_DATE_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;

    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for 4-byte Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

// BIGINT and TIMESTAMP
Value::Value(TypeId type, int64_t i) : Value(type) {
  switch (type) {
    case TypeId::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::TINYINT:
      value_.tinyint = i;
      size_.len =
          (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::SMALLINT:
      value_.smallint = i;
      size_.len =
          (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::INTEGER:
    case TypeId::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len =
          (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::BIGINT:
      value_.bigint = i;
      size_.len =
          (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for 8-byte Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

// BIGINT
Value::Value(TypeId type, uint64_t i) : Value(type) {
  switch (type) {
    case TypeId::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case TypeId::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg = StringUtil::Format(
          "Invalid Type '%s' for unsigned 8-byte Value constructor",
          TypeIdToString(type).c_str());
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

// DECIMAL
Value::Value(TypeId type, double d) : Value(type) {
  switch (type) {
    case TypeId::DECIMAL:
      value_.decimal = d;
      size_.len =
          (value_.decimal == PELOTON_DECIMAL_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for double Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

Value::Value(TypeId type, float f) : Value(type) {
  switch (type) {
    case TypeId::DECIMAL:
      value_.decimal = f;
      size_.len =
          (value_.decimal == PELOTON_DECIMAL_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for float Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

// VARCHAR and VARBINARY
Value::Value(TypeId type, const char *data, uint32_t len, bool manage_data)
    : Value(type) {
  switch (type) {
    case TypeId::VARCHAR:
    case TypeId::VARBINARY:
      if (data == nullptr) {
        value_.varlen = nullptr;
        size_.len = PELOTON_VALUE_NULL;
      } else {
        manage_data_ = manage_data;
        if (manage_data_) {
          PL_ASSERT(len < PELOTON_VARCHAR_MAX_LEN);
          value_.varlen = new char[len];
          PL_ASSERT(value_.varlen != nullptr);
          size_.len = len;
          PL_MEMCPY(value_.varlen, data, len);
        } else {
          // FUCK YOU GCC I do what I want.
          value_.const_varlen = data;
          size_.len = len;
        }
      }
      break;
    default: {
      std::string msg = StringUtil::Format(
          "Invalid Type '%s' for variable-length Value constructor",
          TypeIdToString(type).c_str());
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

Value::Value(TypeId type, const std::string &data) : Value(type) {
  switch (type) {
    case TypeId::VARCHAR:
    case TypeId::VARBINARY: {
      manage_data_ = true;
      // TODO: How to represent a null string here?
      uint32_t len = data.length() + (type == TypeId::VARCHAR);
      value_.varlen = new char[len];
      PL_ASSERT(value_.varlen != nullptr);
      size_.len = len;
      PL_MEMCPY(value_.varlen, data.c_str(), len);
      break;
    }
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for string Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

Value::Value() : Value(TypeId::INVALID) {}

Value::~Value() {
  switch (type_id_) {
    case TypeId::VARBINARY:
    case TypeId::VARCHAR:
      if (manage_data_) {
        delete[] value_.varlen;
      }
      break;
    default:
      break;
  }
}

const std::string Value::GetInfo() const {
  std::ostringstream os;
  os << TypeIdToString(type_id_);
  if (type_id_ == TypeId::VARBINARY || type_id_ == TypeId::VARCHAR) {
    os << "[" << GetLength() << "]";
  }
  os << "(" << ToString() << ")";
  return os.str();
}

bool Value::CheckComparable(const Value &o) const {
  LOG_TRACE("Checking whether '%s' is comparable to '%s'",
            TypeIdToString(GetTypeId()).c_str(),
            TypeIdToString(o.GetTypeId()).c_str());
  switch (GetTypeId()) {
    case TypeId::BOOLEAN:
      switch (o.GetTypeId()) {
        case TypeId::BOOLEAN:
        case TypeId::VARCHAR:
          return (true);
        default:
          break;
      }  // SWITCH
      break;
    case TypeId::TINYINT:
    case TypeId::SMALLINT:
    case TypeId::INTEGER:
    case TypeId::BIGINT:
    case TypeId::DECIMAL:
      switch (o.GetTypeId()) {
        case TypeId::TINYINT:
        case TypeId::SMALLINT:
        case TypeId::INTEGER:
        case TypeId::BIGINT:
        case TypeId::DECIMAL:
        case TypeId::VARCHAR:
          return true;
        default:
          break;
      }  // SWITCH
      break;
    case TypeId::VARCHAR:
      // Anything can be cast to a string!
      return true;
      break;
    case TypeId::VARBINARY:
      if (o.GetTypeId() == TypeId::VARBINARY) return true;
      break;
    case TypeId::TIMESTAMP:
      if (o.GetTypeId() == TypeId::TIMESTAMP) return true;
      break;
    case TypeId::DATE:
      if (o.GetTypeId() == TypeId::DATE) return true;
      break;
    default:
      break;
  }  // SWITCH
  return false;
}

bool Value::CheckInteger() const {
  switch (GetTypeId()) {
    case TypeId::TINYINT:
    case TypeId::SMALLINT:
    case TypeId::INTEGER:
    case TypeId::BIGINT:
    case TypeId::PARAMETER_OFFSET:
      return true;
    default:
      break;
  }

  return false;
}

}  // namespace peloton
}  // namespace type
