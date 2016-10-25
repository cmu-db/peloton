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

#include "common/boolean_type.h"
#include "common/decimal_type.h"
#include "common/numeric_type.h"
#include "common/timestamp_type.h"
#include "common/varlen_type.h"

namespace peloton {
namespace common {

Value::Value(const Value& other){
  type_ = other.type_;
  switch(type_->GetTypeId()){
  case Type::VARCHAR:
  case Type::VARBINARY:
    value_.varlen.len = other.value_.varlen.len;
    value_.varlen.data = new char[value_.varlen.len];
    PL_MEMCPY(value_.varlen.data, other.value_.varlen.data, value_.varlen.len);
    break;
  default:
    value_ = other.value_;
  }
}

Value::Value(Value&& other) : Value(){
  swap(*this, other);
}

Value& Value::operator=(Value other){
  swap(*this, other);
  return *this;
}

// ARRAY is implemented in the header to ease template creation

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
  case Type::SMALLINT:
    value_.smallint = i;
    break;
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    value_.integer = i;
    break;
  case Type::BIGINT:
    value_.bigint = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor");
  }
}

//SMALLINT
Value::Value(Type::TypeId type, int16_t i) :
    Value(type) {
  switch (type) {
  case Type::BOOLEAN:
      value_.boolean = i;
      break;
  case Type::TINYINT:
    value_.tinyint = i;
    break;
  case Type::SMALLINT:
    value_.smallint = i;
    break;
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    value_.integer = i;
    break;
  case Type::BIGINT:
    value_.bigint = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor");
  }
}

//INTEGER and PARAMETER_OFFSET
Value::Value(Type::TypeId type, int32_t i) :
    Value(type) {
  switch (type) {
  case Type::BOOLEAN:
    value_.boolean = i;
    break;
  case Type::TINYINT:
    value_.tinyint = i;
    break;
  case Type::SMALLINT:
    value_.smallint = i;
    break;
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    value_.integer = i;
    break;
  case Type::BIGINT:
    value_.bigint = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor");
  }
}

//BIGINT and TIMESTAMP
Value::Value(Type::TypeId type, int64_t i) :
    Value(type) {
  switch (type) {
  case Type::BOOLEAN:
    value_.boolean = i;
    break;
  case Type::TINYINT:
    value_.tinyint = i;
    break;
  case Type::SMALLINT:
    value_.smallint = i;
    break;
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    value_.integer = i;
    break;
  case Type::BIGINT:
    value_.bigint = i;
    break;
  case Type::TIMESTAMP:
    value_.timestamp = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor");
  }
}

//BIGINT
Value::Value(Type::TypeId type, uint64_t i) :
    Value(type) {
  switch (type) {
  case Type::BOOLEAN:
    value_.boolean = i;
    break;
  case Type::TIMESTAMP:
    value_.timestamp = i;
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor");
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
        "Invalid Type for constructor");
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
        "Invalid Type for constructor");
  }
}

//VARCHAR and VARBINARY
Value::Value(Type::TypeId type, const char *data, uint32_t len) :
    Value(type) {
  switch (type) {
  case Type::VARCHAR:
  case Type::VARBINARY:
    PL_ASSERT(len < PELOTON_VARCHAR_MAX_LEN);
    value_.varlen.data = new char[len];
    PL_ASSERT(value_.varlen.data != nullptr);
    value_.varlen.len = len;
    PL_MEMCPY(value_.varlen.data, data, len);
    break;
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor");
  }
}

Value::Value(Type::TypeId type, const std::string &data) :
    Value(type) {
  switch (type) {
  case Type::VARCHAR:
  case Type::VARBINARY: {
    uint32_t len = data.length() + (type == Type::VARCHAR);
    value_.varlen.data = new char[len];
    PL_ASSERT(value_.varlen.data != nullptr);
    value_.varlen.len = len;
    PL_MEMCPY(value_.varlen.data, data.c_str(), len);
    break;
  }
  default:
    throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
        "Invalid Type for constructor");
  }
}

Value::Value() :
    Value(Type::INVALID) {
}

Value::~Value() {
  switch (type_->GetTypeId()) {
  case Type::VARBINARY:
  case Type::VARCHAR:
    delete[] value_.varlen.data;
    break;
  default:
    break;
  }
}

const std::string Value::GetInfo() const {
  std::ostringstream os;

  os << "\tValue :: " << " type = "
      << Type::GetInstance(GetTypeId())->ToString() << "," << " value = "
      << ToString() << std::endl;

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
    return value_.varlen.len == 0;
  default:
    break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Unknown type.");
}

void Value::CheckComparable(const Value &o) const {
  switch (GetTypeId()) {
  case Type::BOOLEAN:
    if (o.GetTypeId() == Type::BOOLEAN)
      return;
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
    if (o.GetTypeId() == Type::VARCHAR)
      return;
    break;
  case Type::VARBINARY:
    if (o.GetTypeId() == Type::VARBINARY)
      return;
    break;
  case Type::TIMESTAMP:
    if (o.GetTypeId() == Type::TIMESTAMP)
      return;
    break;
  default:
    break;
  }
  std::string msg = "Operation between "
      + Type::GetInstance(GetTypeId())->ToString() + " and "
      + Type::GetInstance(o.GetTypeId())->ToString() + " is invalid.";
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
  std::string msg = "Type " + Type::GetInstance(GetTypeId())->ToString()
      + " is not an integer type.";
  throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
}

Value Value::DeserializeFrom(const char *storage, const Type::TypeId type_id,
UNUSED_ATTRIBUTE const bool inlined,
UNUSED_ATTRIBUTE VarlenPool *pool) {
  return Type::GetInstance(type_id)->DeserializeFrom(storage, inlined, pool);
}

Value Value::DeserializeFrom(SerializeInput &in, const Type::TypeId type_id,
    VarlenPool *pool UNUSED_ATTRIBUTE) {
  return Type::GetInstance(type_id)->DeserializeFrom(in, pool);
}

Value Value::CompareEquals(const Value &o) const {
  return type_->CompareEquals(*this, o);
}
Value Value::CompareNotEquals(const Value &o) const {
  return type_->CompareNotEquals(*this, o);
}
Value Value::Value::CompareLessThan(const Value &o) const {
  return type_->CompareLessThan(*this, o);
}
Value Value::CompareLessThanEquals(const Value &o) const {
  return type_->CompareLessThanEquals(*this, o);
}
Value Value::CompareGreaterThan(const Value &o) const {
  return type_->CompareGreaterThan(*this, o);
}
Value Value::CompareGreaterThanEquals(const Value &o) const {
  return type_->CompareGreaterThanEquals(*this, o);
}

// Other mathematical functions
Value Value::Add(const Value &o) const {
  return type_->Add(*this, o);
}
Value Value::Subtract(const Value &o) const {
  return type_->Subtract(*this, o);
}
Value Value::Multiply(const Value &o) const {
  return type_->Multiply(*this, o);
}
Value Value::Divide(const Value &o) const {
  return type_->Divide(*this, o);
}
Value Value::Modulo(const Value &o) const {
  return type_->Modulo(*this, o);
}
Value Value::Min(const Value &o) const {
  return type_->Min(*this, o);
}
Value Value::Max(const Value &o) const {
  return type_->Max(*this, o);
}
Value Value::Sqrt() const {
  return type_->Sqrt(*this);
}
Value Value::OperateNull(const Value &o) const {
  return type_->OperateNull(*this, o);
}
bool Value::IsZero() const {
  return type_->IsZero(*this);
}

// Is the data inlined into this classes storage space, or must it be accessed
// through an indirection/pointer?
bool Value::IsInlined() const {
  return type_->IsInlined(*this);
}

// Return a stringified version of this value
std::string Value::ToString() const {
  return type_->ToString(*this);
}

// Compute a hash value
size_t Value::Hash() const {
  return type_->Hash(*this);
}
void Value::HashCombine(size_t &seed) const {
  return type_->HashCombine(*this, seed);
}

// Serialize this value into the given storage space. The inlined parameter
// indicates whether we are allowed to inline this value into the storage
// space, or whether we must store only a reference to this value. If inlined
// is false, we may use the provided data pool to allocate space for this
// value, storing a reference into the allocated pool space in the storage.
void Value::SerializeTo(char *storage, bool inlined, VarlenPool *pool) const {
  type_->SerializeTo(*this, storage, inlined, pool);
}
void Value::SerializeTo(SerializeOutput &out) const {
  type_->SerializeTo(*this, out);
}

// Create a copy of this value
Value Value::Copy() const {
  return type_->Copy(*this);
}

Value Value::CastAs(const Type::TypeId type_id) const {
  return type_->CastAs(*this, type_id);
}

// Access the raw variable length data
const char *Value::GetData() const {
  return type_->GetData(*this);
}

// Get the length of the variable length data
uint32_t Value::GetLength() const {
  return type_->GetLength(*this);
}

// Get the element at a given index in this array
Value Value::GetElementAt(uint64_t idx) const {
  return type_->GetElementAt(*this, idx);
}

Type::TypeId Value::GetElementType() const {
  return type_->GetElementType(*this);
}

// Does this value exist in this array?
Value Value::InList(const Value &object) const {
  return type_->InList(*this, object);
}

}  // namespace peloton
}  // namespace common
