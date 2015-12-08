//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tuple.cpp
//
// Identification: src/backend/storage/tuple.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/tuple.h"

#include <cstdlib>
#include <sstream>
#include <cassert>

#include "backend/storage/tuple.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/catalog/schema.h"

namespace peloton {
namespace storage {

// Does not delete SCHEMA
Tuple::~Tuple(){

  // delete the tuple data
   if (allocated) delete[] tuple_data;
 }

// Get the value of a specified column (const)
Value Tuple::GetValue(const oid_t column_id) const {
  assert(tuple_schema);
  assert(tuple_data);

  const ValueType column_type = tuple_schema->GetType(column_id);

  const char *data_ptr = GetDataPtr(column_id);
  const bool is_inlined = tuple_schema->IsInlined(column_id);

  return std::move(Value::InitFromTupleStorage(data_ptr, column_type, is_inlined));
}

// Set all columns by value into this tuple.
void Tuple::SetValue(const oid_t column_id, const Value& value,
                     VarlenPool *dataPool) {
  assert(tuple_schema);
  assert(tuple_data);

  const ValueType type = tuple_schema->GetType(column_id);

  const bool is_inlined = tuple_schema->IsInlined(column_id);
  char *dataPtr = GetDataPtr(column_id);
  int32_t column_length = tuple_schema->GetLength(column_id);

  if (is_inlined == false)
    column_length = tuple_schema->GetVariableLength(column_id);

  const bool is_in_bytes = false;
  if(dataPool == nullptr) {
    if(type == value.GetValueType()) {
      value.SerializeToTupleStorage(dataPtr, is_inlined, column_length, is_in_bytes);
    }
    else {
      Value casted_value = value.CastAs(type);
      casted_value.SerializeToTupleStorage(dataPtr, is_inlined, column_length, is_in_bytes);
      casted_value.SetCleanUp(false);
    }
  }
  else {
    if(type == value.GetValueType()) {
      // TODO: Not sure about arguments
      value.SerializeToTupleStorageAllocateForObjects(dataPtr, is_inlined, column_length,
                                                      is_in_bytes, dataPool);
    }
    else {
      value.CastAs(type).SerializeToTupleStorageAllocateForObjects(dataPtr, is_inlined, column_length,
                                                                   is_in_bytes, dataPool);
    }
  }

}

void Tuple::SetFromTuple(const storage::Tuple *tuple,
                         const std::vector<oid_t> &columns,
                         VarlenPool *pool) {
  // We don't do any checks here about the source tuple and
  // this tuple's schema
  oid_t this_col_itr = 0;
  for (auto col : columns) {
    SetValue(this_col_itr, tuple->GetValue(col), pool);
    this_col_itr++;
  }
}

// For an insert, the copy should do an allocation for all uninlinable columns
// This does not do any schema checks. They must match.
void Tuple::Copy(const void *source, VarlenPool *pool) {
  assert(tuple_schema);
  assert(tuple_data);

  const bool is_inlined = tuple_schema->IsInlined();
  const oid_t uninlineable_column_count =
      tuple_schema->GetUninlinedColumnCount();

  if (is_inlined) {
    // copy the data
    ::memcpy(tuple_data, source, tuple_schema->GetLength());
  } else {
    // copy the data
    ::memcpy(tuple_data, source, tuple_schema->GetLength());

    // Copy each uninlined column doing an allocation for copies.
    for (oid_t column_itr = 0; column_itr < uninlineable_column_count;
        column_itr++) {
      const oid_t unlineable_column_id =
          tuple_schema->GetUninlinedColumn(column_itr);

      // Get original value from uninlined pool
      Value value = GetValue(unlineable_column_id);

      // Make a copy of the value at a new location in uninlined pool
      SetValue(unlineable_column_id, value, pool);
    }
  }
}

/**
 * Determine the maximum number of bytes when serialized for Export.
 * Excludes the bytes required by the row header (which includes
 * the null bit indicators) and ignores the width of metadata columns.
 */
size_t Tuple::ExportSerializationSize() const {
  size_t bytes = 0;
  int column_count = GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; ++column_itr) {
    switch (GetType(column_itr)) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
      case VALUE_TYPE_DOUBLE:
        bytes += sizeof(int64_t);
        break;

      case VALUE_TYPE_DECIMAL:
        // Decimals serialized in ascii as
        // 32 bits of length + max prec digits + radix pt + sign
        bytes += sizeof(int32_t) + Value::kMaxDecPrec + 1 + 1;
        break;

      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
        // 32 bit length preceding value and
        // actual character data without null string terminator.
        if (!GetValue(column_itr).IsNull()) {
          bytes += (sizeof(int32_t) +
              ValuePeeker::PeekObjectLengthWithoutNull(GetValue(column_itr)));
        }
        break;

      default:
        throw UnknownTypeException(
            GetType(column_itr),
            "Unknown ValueType found during Export serialization.");
        return (size_t)0;
    }
  }
  return bytes;
}

// Return the amount of memory allocated for non-inlined objects
size_t Tuple::GetUninlinedMemorySize() const {
  size_t bytes = 0;
  int column_count = GetColumnCount();

  // fast-path for no inlined cols
  if (tuple_schema->IsInlined() == false) {
    for (int column_itr = 0; column_itr < column_count; ++column_itr) {
      // peekObjectLength is unhappy with non-varchar
      if ((GetType(column_itr) == VALUE_TYPE_VARCHAR ||
          (GetType(column_itr) == VALUE_TYPE_VARBINARY)) &&
          !tuple_schema->IsInlined(column_itr)) {
        if (!GetValue(column_itr).IsNull()) {
          bytes += (sizeof(int32_t) +
              ValuePeeker::PeekObjectLengthWithoutNull(GetValue(column_itr)));
        }
      }
    }
  }

  return bytes;
}

void Tuple::DeserializeFrom(SerializeInputBE &input, VarlenPool *dataPool) {
  assert(tuple_schema);
  assert(tuple_data);

  input.ReadInt();
  const int column_count = tuple_schema->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const ValueType type = tuple_schema->GetType(column_itr);

    /**
     * DeserializeFrom is only called when we serialize/deserialize tables.
     * The serialization format for Strings/Objects in a serialized table
     * happens to have the same in memory representation as the Strings/Objects
     * in a Tuple. The goal here is to wrap the serialized representation of
     * the value in an Value and then serialize that into the tuple from the
     * Value. This makes it possible to push more value specific functionality
     * out of Tuple. The memory allocation will be performed when serializing
     * to tuple storage.
     */
    const bool is_inlined = tuple_schema->IsInlined(column_itr);
    int32_t column_length;
    char *data_ptr = GetDataPtr(column_itr);

    if( is_inlined ){
      column_length = tuple_schema->GetLength(column_itr);
    }else{
      column_length = tuple_schema->GetVariableLength(column_itr);
    }

    // TODO: Not sure about arguments
    const bool is_in_bytes = false;
    Value::DeserializeFrom(input, dataPool, data_ptr, type,
                           is_inlined, column_length,
                           is_in_bytes);
  }
}

void Tuple::DeserializeWithHeaderFrom(SerializeInputBE &input) {

  assert(tuple_schema);
  assert(tuple_data);

  input.ReadInt();  // Read in the tuple size, discard

  const int column_count = tuple_schema->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const ValueType type = tuple_schema->GetType(column_itr);

    const bool is_inlined = tuple_schema->IsInlined(column_itr);
    char *data_ptr = GetDataPtr(column_itr);
    const int32_t column_length = tuple_schema->GetLength(column_itr);

    // TODO: Not sure about arguments
    const bool is_in_bytes = false;
    Value::DeserializeFrom(
        input, NULL, data_ptr, type,
        is_inlined, column_length,
        is_in_bytes);
  }

}

void Tuple::SerializeWithHeaderTo(SerializeOutput &output) {
  assert(tuple_schema);
  assert(tuple_data);

  size_t start = output.Position();
  output.WriteInt(0);  // reserve first 4 bytes for the total tuple size

  const int column_count = tuple_schema->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value value = GetValue(column_itr);
    value.SerializeTo(output);
  }

  int32_t serialized_size =
      static_cast<int32_t>(output.Position() - start - sizeof(int32_t));

  // write out the length of the tuple at start
  output.WriteIntAt(start, serialized_size);
}

void Tuple::SerializeTo(SerializeOutput &output) {
  size_t start = output.ReserveBytes(4);
  const int column_count = tuple_schema->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value value = GetValue(column_itr);
    value.SerializeTo(output);
  }

  output.WriteIntAt(
      start, static_cast<int32_t>(output.Position() - start - sizeof(int32_t)));
}

void Tuple::SerializeToExport(ExportSerializeOutput &output, int colOffset,
                              uint8_t *null_array) {
  const int column_count = GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    // NULL doesn't produce any bytes for the Value
    // Handle it here to consolidate manipulation of the nullarray.
    if (IsNull(column_itr)) {
      // turn on relevant bit in nullArray
      int byte = (colOffset + column_itr) >> 3;
      int bit = (colOffset + column_itr) % 8;
      int mask = 0x80 >> bit;
      null_array[byte] = (uint8_t)(null_array[byte] | mask);
      continue;
    }

    GetValue(column_itr).SerializeToExportWithoutNull(output);
  }
}

bool Tuple::operator==(const Tuple &other) const {
  if (tuple_schema != other.tuple_schema) {
    return false;
  }

  return EqualsNoSchemaCheck(other);
}

bool Tuple::operator!=(const Tuple &other) const { return !(*this == other); }

bool Tuple::EqualsNoSchemaCheck(const Tuple &other) const {
  const int column_count = tuple_schema->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const Value lhs = GetValue(column_itr);
    const Value rhs = other.GetValue(column_itr);
    if (lhs.OpNotEquals(rhs).IsTrue()) {
      return false;
    }
  }

  return true;
}

bool Tuple::EqualsNoSchemaCheck(const Tuple &other,
                                const std::vector<oid_t> &columns) const {
  for (auto column_itr : columns) {
    const Value lhs = GetValue(column_itr);
    const Value rhs = other.GetValue(column_itr);
    if (lhs.OpNotEquals(rhs).IsTrue()) {
      return false;
    }
  }

  return true;
}

void Tuple::SetAllNulls() {
  assert(tuple_schema);
  assert(tuple_data);
  const int column_count = tuple_schema->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value value = Value::GetNullValue(tuple_schema->GetType(column_itr));
    SetValue(column_itr, value, nullptr);
  }
}

int Tuple::Compare(const Tuple &other) const {
  int diff;
  const int column_count = tuple_schema->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const Value lhs = GetValue(column_itr);
    const Value rhs = other.GetValue(column_itr);
    diff = lhs.Compare(rhs);

    if (diff) {
      return diff;
    }
  }

  return 0;
}

int Tuple::Compare(const Tuple &other,
                   const std::vector<oid_t> &columns) const {
  int diff;

  for (auto column_itr : columns) {
    const Value lhs = GetValue(column_itr);
    const Value rhs = other.GetValue(column_itr);
    diff = lhs.Compare(rhs);

    if (diff) {
      return diff;
    }
  }

  return 0;
}

size_t Tuple::HashCode(size_t seed) const {
  const int column_count = tuple_schema->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const Value value = GetValue(column_itr);
    value.HashCombine(seed);
  }

  return seed;
}

void Tuple::MoveToTuple(const void *tuple_data_) {
  assert(tuple_schema);
  tuple_data = reinterpret_cast<char *>(const_cast<void *>(tuple_data_));
}

size_t Tuple::HashCode() const {
  size_t seed = 0;
  return HashCode(seed);
}

char *Tuple::GetDataPtr(const oid_t column_id) {
  assert(tuple_schema);
  assert(tuple_data);
  return &tuple_data[tuple_schema->GetOffset(column_id)];
}

const char *Tuple::GetDataPtr(const oid_t column_id) const {
  assert(tuple_schema);
  assert(tuple_data);
  return &tuple_data[tuple_schema->GetOffset(column_id)];
}

std::string Tuple::GetInfo() const {
  std::stringstream os;

  oid_t column_count = GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    os << "(";
    if (IsNull(column_itr)) {
      os << "<NULL>";
    } else {
      os << GetValue(column_itr);
    }
    os << ")";
  }

  os << std::endl;
  return os.str();
}

std::ostream &operator<<(std::ostream &os, const Tuple &tuple) {
  os << tuple.GetInfo();
  return os;
}

}  // End storage namespace
}  // End peloton namespace
