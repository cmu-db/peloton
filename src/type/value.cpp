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
    case Type::VARCHAR:
    case Type::VARBINARY:
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
Value::Value(Type::TypeId type, int8_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TINYINT:
      value_.tinyint = i;
      size_.len =
          (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::SMALLINT:
      value_.smallint = i;
      size_.len =
          (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len =
          (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::BIGINT:
      value_.bigint = i;
      size_.len =
          (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for 1-byte Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, msg);
    }
  }
}

// SMALLINT
Value::Value(Type::TypeId type, int16_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TINYINT:
      value_.tinyint = i;
      size_.len =
          (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::SMALLINT:
      value_.smallint = i;
      size_.len =
          (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len =
          (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::BIGINT:
      value_.bigint = i;
      size_.len =
          (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for 2-byte Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, msg);
    }
  }
}

// INTEGER and PARAMETER_OFFSET
Value::Value(Type::TypeId type, int32_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TINYINT:
      value_.tinyint = i;
      size_.len =
          (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::SMALLINT:
      value_.smallint = i;
      size_.len =
          (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len =
          (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::BIGINT:
      value_.bigint = i;
      size_.len =
          (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::DATE:
      value_.date = i;
      size_.len = (value_.date == PELOTON_DATE_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;

    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for 4-byte Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, msg);
    }
  }
}

// BIGINT and TIMESTAMP
Value::Value(Type::TypeId type, int64_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TINYINT:
      value_.tinyint = i;
      size_.len =
          (value_.tinyint == PELOTON_INT8_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::SMALLINT:
      value_.smallint = i;
      size_.len =
          (value_.smallint == PELOTON_INT16_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      value_.integer = i;
      size_.len =
          (value_.integer == PELOTON_INT32_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::BIGINT:
      value_.bigint = i;
      size_.len =
          (value_.bigint == PELOTON_INT64_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for 8-byte Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, msg);
    }
  }
}

// BIGINT
Value::Value(Type::TypeId type, uint64_t i) : Value(type) {
  switch (type) {
    case Type::BOOLEAN:
      value_.boolean = i;
      size_.len =
          (value_.boolean == PELOTON_BOOLEAN_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    case Type::TIMESTAMP:
      value_.timestamp = i;
      size_.len =
          (value_.timestamp == PELOTON_TIMESTAMP_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg = StringUtil::Format(
          "Invalid Type '%s' for unsigned 8-byte Value constructor",
          TypeIdToString(type).c_str());
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, msg);
    }
  }
}

// DECIMAL
Value::Value(Type::TypeId type, double d) : Value(type) {
  switch (type) {
    case Type::DECIMAL:
      value_.decimal = d;
      size_.len =
          (value_.decimal == PELOTON_DECIMAL_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for double Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, msg);
    }
  }
}

Value::Value(Type::TypeId type, float f) : Value(type) {
  switch (type) {
    case Type::DECIMAL:
      value_.decimal = f;
      size_.len =
          (value_.decimal == PELOTON_DECIMAL_NULL ? PELOTON_VALUE_NULL : 0);
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%s' for float Value constructor",
                             TypeIdToString(type).c_str());
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, msg);
    }
  }
}

// VARCHAR and VARBINARY
Value::Value(Type::TypeId type, const char *data, uint32_t len,
             bool manage_data)
    : Value(type) {
  switch (type) {
    case Type::VARCHAR:
    case Type::VARBINARY:
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
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, msg);
    }
  }
}

Value::Value(Type::TypeId type, const std::string &data) : Value(type) {
  switch (type) {
    case Type::VARCHAR:
    case Type::VARBINARY: {
      manage_data_ = true;
      // TODO: How to represent a null string here?
      uint32_t len = data.length() + (type == Type::VARCHAR);
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
      throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, msg);
    }
  }
}

Value::Value() : Value(Type::INVALID) {}

Value::~Value() {
  switch (type_id_) {
    case Type::VARBINARY:
    case Type::VARCHAR:
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
  if (type_id_ == Type::VARBINARY || type_id_ == Type::VARCHAR) {
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
    case Type::BOOLEAN:
      switch (o.GetTypeId()) {
        case Type::BOOLEAN:
        case Type::VARCHAR:
          return (true);
        default:break;
      } // SWITCH
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
        case Type::VARCHAR:
          return true;
        default:
          break;
      } // SWITCH
      break;
    case Type::VARCHAR:
      // Anything can be cast to a string!
      return true;
      break;
    case Type::VARBINARY:
      if (o.GetTypeId() == Type::VARBINARY) return true;
      break;
    case Type::TIMESTAMP:
      if (o.GetTypeId() == Type::TIMESTAMP) return true;
      break;
    default:
      break;
  } // SWITCH
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

}  // namespace peloton
}  // namespace type
