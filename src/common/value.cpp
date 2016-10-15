//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value.h
//
// Identification: src/backend/common/value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/value.h"
#include "common/boolean_value.h"
#include "common/decimal_value.h"
#include "common/numeric_value.h"
#include "common/timestamp_value.h"
#include "common/varlen_value.h"

namespace peloton {
namespace common {

// BOOLEAN and TINYINT
Value::Value(Type::TypeId type, int8_t i) :
    Value(type) {
  switch (type) {
  case Type::BOOLEAN:
    value_.boolean = i;
    break;
  case Type::TINYINT:
    value_.tinyint = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor: %s", Type::GetInstance(type).ToString());
  }
}

//SMALLINT
Value::Value(Type::TypeId type, int16_t i) :
    Value(type) {
  switch (type) {
  case Type::SMALLINT:
    value_.smallint = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor: %s", Type::GetInstance(type).ToString());
  }
}

//INTEGER and PARAMETER_OFFSET
Value::Value(Type::TypeId type, int32_t i) :
    Value(type) {
  switch (type) {
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    value_.integer = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor: %s", Type::GetInstance(type).ToString());
  }
}

//BIGINT and TIMESTAMP
Value::Value(Type::TypeId type, int64_t i) :
    Value(type) {
  switch (type) {
  case Type::BIGINT:
    value_.bigint = i;
    break;
  case Type::TIMESTAMP:
    value_.timestamp = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor: %s", Type::GetInstance(type).ToString());
  }
}

//BIGINT
Value::Value(Type::TypeId type, uint64_t i) :
    Value(type) {
  switch (type) {
  case Type::TIMESTAMP:
    value_.timestamp = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor: %s", Type::GetInstance(type).ToString());
  }
}

//DECIMAL
Value::Value(Type::TypeId type, double d) :
    Value(type) {
  switch (type) {
  case Type::DECIMAL:
    value_.decimal = d;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor: %s", Type::GetInstance(type).ToString());
  }
}

Value::Value(Type::TypeId type, float f) :
    Value(type) {
  switch (type) {
  case Type::DECIMAL:
    value_.decimal = f;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor: %s", Type::GetInstance(type).ToString());
  }
}

//VARCHAR and VARBINARY
Value::Value(Type::TypeId type, const char *data, uint32_t len) :
    Value(type) {
  switch (type) {
  case Type::VARCHAR:
  case Type::VARBINARY:
    if (len == PELOTON_VARCHAR_MAX_LEN) {
        value_.ptr = new char[sizeof(uint32_t)];
        PL_ASSERT(value_.ptr != nullptr);
        (*(uint32_t *) value_.ptr) = len;
      } else {
        value_.ptr = new char[len + sizeof(uint32_t)];
        PL_ASSERT(value_.ptr != nullptr);
        (*(uint32_t *) value_.ptr) = len;
        char *dest = value_.ptr + sizeof(uint32_t);
        PL_MEMCPY(dest, data, len);
      }
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor: %s", Type::GetInstance(type).ToString());
  }
}

Value::Value(Type::TypeId type, const std::string &data) :
    Value(type) {
  switch (type) {
  case Type::VARCHAR:
  case Type::VARBINARY:
    uint32_t len = data.length() + (type == Type::VARCHAR);
    value_.ptr = new char[len + sizeof(uint32_t)];
    PL_ASSERT(value_.ptr != nullptr);
    (*(uint32_t *) value_.ptr) = len;
    char *dest = value_.ptr + sizeof(uint32_t);
    PL_MEMCPY(dest, data.c_str(), len);
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor: %s", Type::GetInstance(type).ToString());
  }
}


Value::~Value() {
  switch(type_->GetTypeId()){
  case Type::VARBINARY:
  case Type::VARCHAR:
    delete[] value_.ptr;
  }
}

const std::string Value::GetInfo() const {
  std::ostringstream os;

  os << "\tValue :: "
     << " type = " << Type::GetInstance(GetTypeId()).ToString() << ","
     << " value = " << ToString() << std::endl;

  return os.str();
}

bool Value::IsNull() const {
  switch (GetTypeId()) {
    case Type::BOOLEAN:
      return (value_.boolean == PELOTON_BOOLEAN_NULL);
    case Type::TINYINT:
      return (value_.tinyint == PELOTON_INT8_NULL);
    case Type::SMALLINT:
      return (value_.smallint == PELOTON_INT16_NULL);
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      return (value_.integer == PELOTON_INT32_NULL);
    case Type::BIGINT:
      return (value_.bigint == PELOTON_INT64_NULL);
    case Type::DECIMAL:
      return (value_.decimal == PELOTON_DECIMAL_NULL);
    case Type::TIMESTAMP:
      return (value_.timestamp == PELOTON_TIMESTAMP_NULL);
    case Type::VARCHAR:
    case Type::VARBINARY:
      return (*((uint32_t *) value_.ptr) == 0);
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Unknown type.");
}

void Value::CheckComparable(const Value &o) const {
  switch (GetTypeId()) {
    case Type::BOOLEAN:
      if (o.GetTypeId() == Type::BOOLEAN) return;
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
          return;
        default:
          break;
      }
      break;
    case Type::VARCHAR:
      if (o.GetTypeId() == Type::VARCHAR) return;
      break;
    case Type::VARBINARY:
      if (o.GetTypeId() == Type::VARBINARY)
        return;
      break;
    case Type::TIMESTAMP:
      if (o.GetTypeId() == Type::TIMESTAMP) return;
      break;
    default:
      break;
  }
  std::string msg =
      "Operation between " + Type::GetInstance(GetTypeId()).ToString() +
      " and " + Type::GetInstance(o.GetTypeId()).ToString() + " is invalid.";
  throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
}

void Value::CheckInteger() const {
  switch (GetTypeId()) {
    case Type::TINYINT:
    case Type::SMALLINT:
    case Type::INTEGER:
    case Type::BIGINT:
    case Type::PARAMETER_OFFSET:
      return;
    default:
      break;
  }
  std::string msg = "Type " + Type::GetInstance(GetTypeId()).ToString() +
                    " is not an integer type.";
  throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
}

Value *Value::DeserializeFrom(const char *storage, const Type::TypeId type_id,
                              UNUSED_ATTRIBUTE const bool inlined,
                              UNUSED_ATTRIBUTE VarlenPool *pool) {
  switch (type_id) {
    case Type::BOOLEAN:
      break;
    case Type::TINYINT: {
      int8_t val = *reinterpret_cast<const int8_t *>(storage);
      return new IntegerValue(val);
    }
    case Type::SMALLINT: {
      int16_t val = *reinterpret_cast<const int16_t *>(storage);
      return new IntegerValue(val);
    }
    case Type::INTEGER: {
      int32_t val = *reinterpret_cast<const int32_t *>(storage);
      return new IntegerValue(val);
    }
    case Type::BIGINT: {
      int64_t val = *reinterpret_cast<const int64_t *>(storage);
      return new IntegerValue(val);
    }
    case Type::DECIMAL: {
      double val = *reinterpret_cast<const double *>(storage);
      return new DecimalValue(val);
    }
    case Type::TIMESTAMP: {
      uint64_t val = *reinterpret_cast<const uint64_t *>(storage);
      return new TimestampValue(val);
    }
    case Type::VARCHAR:
    case Type::VARBINARY:{
      bool isBinary = type_id == Type::VARBINARY;
      const char *ptr = *reinterpret_cast<const char *const *>(storage);
      if (ptr == nullptr) return new VarlenValue(nullptr, 0, isBinary);
      uint32_t len = *reinterpret_cast<const uint32_t *>(ptr);
      return new VarlenValue(ptr + sizeof(uint32_t), len, isBinary);
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Unknown type.");
}

Value *Value::DeserializeFrom(SerializeInput &in, const Type::TypeId type_id,
                              VarlenPool *pool UNUSED_ATTRIBUTE) {
  switch (type_id) {
    case Type::BOOLEAN:
      break;
    case Type::TINYINT:
      return new IntegerValue(in.ReadByte());
    case Type::SMALLINT:
      return new IntegerValue(in.ReadShort());
    case Type::INTEGER:
      return new IntegerValue(in.ReadInt());
    case Type::BIGINT:
      return new IntegerValue(in.ReadLong());
    case Type::DECIMAL:
      return new DecimalValue(in.ReadDouble());
    case Type::TIMESTAMP:
      return new IntegerValue(in.ReadLong());
    case Type::VARCHAR:
    case Type::VARBINARY:{
      bool isBinary = type_id == Type::VARBINARY;
      uint32_t len = in.ReadInt();
      const char *data = (char *)in.getRawPointer(len);
      return new VarlenValue(data, len, isBinary);
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Unknown type.");
}

}  // namespace peloton
}  // namespace common
