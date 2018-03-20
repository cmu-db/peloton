//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple.cpp
//
// Identification: src/storage/tuple.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/tuple.h"

#include <cstdlib>
#include <sstream>

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace storage {

// Does not delete SCHEMA
Tuple::~Tuple() {
  // delete the tuple data
  if (allocated_) delete[] tuple_data_;
}

// Get the value of a specified column (const)
type::Value Tuple::GetValue(oid_t column_id) const {
  PL_ASSERT(tuple_schema_);
  PL_ASSERT(tuple_data_);
  const type::TypeId column_type = tuple_schema_->GetType(column_id);
  const char *data_ptr = GetDataPtr(column_id);
  const bool is_inlined = tuple_schema_->IsInlined(column_id);
  return type::Value::DeserializeFrom(data_ptr, column_type, is_inlined);
}

// Set all columns by value into this tuple.
void Tuple::SetValue(const oid_t column_offset, const type::Value &value,
                     type::AbstractPool *data_pool) {
  const type::TypeId type = tuple_schema_->GetType(column_offset);
  LOG_TRACE("c offset: %d; using pool: %p", column_offset, data_pool);

  const bool is_inlined = tuple_schema_->IsInlined(column_offset);
  char *value_location = GetDataPtr(column_offset);
  UNUSED_ATTRIBUTE size_t column_length =
      tuple_schema_->GetLength(column_offset);

  LOG_TRACE("column_offset: %d; value_location %p; column_length %lu; type %s",
            column_offset, value_location, column_length,
            TypeIdToString(type).c_str());

  // Skip casting if type is same
  if (type == value.GetTypeId()) {
    if ((type == type::TypeId::VARCHAR || type == type::TypeId::VARBINARY)
        && (column_length != 0 && value.GetLength() != type::PELOTON_VALUE_NULL)
        && value.GetLength() > column_length + 1) {      // value.GetLength() == strlen(value) + 1 because of '\0'
      throw peloton::ValueOutOfRangeException(type, column_length);
    }
    value.SerializeTo(value_location, is_inlined, data_pool);
  } else {
    type::Value casted_value = (value.CastAs(type));
    casted_value.SerializeTo(value_location, is_inlined, data_pool);
  }
}

void Tuple::SetFromTuple(const AbstractTuple *tuple,
                         const std::vector<oid_t> &columns,
                         type::AbstractPool *pool) {
  // We don't do any checks here about the source tuple and
  // this tuple's schema
  oid_t this_col_itr = 0;
  for (auto col : columns) {
    type::Value fetched_value = (tuple->GetValue(col));
    SetValue(this_col_itr, fetched_value, pool);
    this_col_itr++;
  }
}

// For an insert, the copy should do an allocation for all uninlinable columns
// This does not do any schema checks. They must match.
void Tuple::Copy(const void *source, type::AbstractPool *pool) {
  const bool is_inlined = tuple_schema_->IsInlined();
  const oid_t uninlineable_column_count =
      tuple_schema_->GetUninlinedColumnCount();

  if (is_inlined) {
    // copy the data
    PL_MEMCPY(tuple_data_, source, tuple_schema_->GetLength());
  } else {
    // copy the data
    PL_MEMCPY(tuple_data_, source, tuple_schema_->GetLength());

    // Copy each uninlined column doing an allocation for copies.
    for (oid_t column_itr = 0; column_itr < uninlineable_column_count;
         column_itr++) {
      const oid_t unlineable_column_id =
          tuple_schema_->GetUninlinedColumn(column_itr);

      // Get original value from uninlined pool
      type::Value value = GetValue(unlineable_column_id);

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
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT:
      case type::TypeId::TIMESTAMP:
      case type::TypeId::DECIMAL:
        // case type::Type::DOUBLE:
        bytes += sizeof(int64_t);
        break;

      // case type::TypeId::DECIMAL:
      // Decimals serialized in ascii as
      // 32 bits of length + max prec digits + radix pt + sign
      // bytes += sizeof(int32_t) + Value::kMaxDecPrec + 1 + 1;
      // break;

      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY:
        // 32 bit length preceding value and
        // actual character data without null string terminator.
        if (!GetValue(column_itr).IsNull()) {
          bytes += (sizeof(int32_t) + GetValue(column_itr).GetLength());
        }
        break;

      default:
        throw UnknownTypeException(
            static_cast<int>(GetType(column_itr)),
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
  if (tuple_schema_->IsInlined() == false) {
    for (int column_itr = 0; column_itr < column_count; ++column_itr) {
      // peekObjectLength is unhappy with non-varchar
      if ((GetType(column_itr) == type::TypeId::VARCHAR ||
           (GetType(column_itr) == type::TypeId::VARBINARY)) &&
          !tuple_schema_->IsInlined(column_itr)) {
        if (!GetValue(column_itr).IsNull()) {
          bytes += (sizeof(int32_t) + GetValue(column_itr).GetLength());
        }
      }
    }
  }

  return bytes;
}

void Tuple::DeserializeFrom(UNUSED_ATTRIBUTE SerializeInput &input,
                            UNUSED_ATTRIBUTE type::AbstractPool *dataPool) {
  /*PL_ASSERT(tuple_schema);
  PL_ASSERT(tuple_data);

  input.ReadInt();
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const ValueType type = tuple_schema_->GetType(column_itr);*/

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
  /*const bool is_inlined = tuple_schema_->IsInlined(column_itr);
  int32_t column_length;
  char *data_ptr = GetDataPtr(column_itr);

  if (is_inlined) {
    column_length = tuple_schema_->GetLength(column_itr);
  } else {
    column_length = tuple_schema_->GetVariableLength(column_itr);
  }

  // TODO: Not sure about arguments
  const bool is_in_bytes = false;
  Value::DeserializeFrom(input, dataPool, data_ptr, type, is_inlined,
                         column_length, is_in_bytes);
}*/
}

void Tuple::DeserializeWithHeaderFrom(SerializeInput &input UNUSED_ATTRIBUTE) {
  /*PL_ASSERT(tuple_schema_);
  PL_ASSERT(tuple_data_);

  input.ReadInt();  // Read in the tuple size, discard

  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const ValueType type = tuple_schema_->GetType(column_itr);

    const bool is_inlined = tuple_schema_->IsInlined(column_itr);
    char *data_ptr = GetDataPtr(column_itr);
    const int32_t column_length = tuple_schema_->GetLength(column_itr);

    // TODO: Not sure about arguments
    const bool is_in_bytes = false;
    Value::DeserializeFrom(input, NULL, data_ptr, type, is_inlined,
                           column_length, is_in_bytes);*/
}

void Tuple::SerializeWithHeaderTo(SerializeOutput &output) {
  PL_ASSERT(tuple_schema_);
  PL_ASSERT(tuple_data_);

  size_t start = output.Position();
  output.WriteInt(0);  // reserve first 4 bytes for the total tuple size

  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    type::Value value = GetValue(column_itr);
    value.SerializeTo(output);
  }

  int32_t serialized_size =
      static_cast<int32_t>(output.Position() - start - sizeof(int32_t));

  // write out the length of the tuple at start
  output.WriteIntAt(start, serialized_size);
}

void Tuple::SerializeTo(SerializeOutput &output) {
  PL_ASSERT(tuple_schema_);
  size_t start = output.ReserveBytes(4);
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    type::Value value(GetValue(column_itr));
    value.SerializeTo(output);
  }

  output.WriteIntAt(
      start, static_cast<int32_t>(output.Position() - start - sizeof(int32_t)));
}

void Tuple::SerializeToExport(SerializeOutput &output, int colOffset,
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

    type::Value val = GetValue(column_itr);
    val.SerializeTo(output);
  }
}

bool Tuple::operator==(const Tuple &other) const {
  if (tuple_schema_ != other.tuple_schema_) {
    return false;
  }

  return EqualsNoSchemaCheck(other);
}

bool Tuple::operator!=(const Tuple &other) const { return !(*this == other); }

bool Tuple::EqualsNoSchemaCheck(const AbstractTuple &other) const {
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    type::Value lhs = (GetValue(column_itr));
    type::Value rhs = (other.GetValue(column_itr));
    if (lhs.CompareNotEquals(rhs) == CmpBool::TRUE) {
      return false;
    }
  }

  return true;
}

bool Tuple::EqualsNoSchemaCheck(const AbstractTuple &other,
                                const std::vector<oid_t> &columns) const {
  for (auto column_itr : columns) {
    type::Value lhs = (GetValue(column_itr));
    type::Value rhs = (other.GetValue(column_itr));
    if (lhs.CompareNotEquals(rhs) == CmpBool::TRUE) {
      return false;
    }
  }

  return true;
}

void Tuple::SetAllNulls() {
  PL_ASSERT(tuple_schema_);
  PL_ASSERT(tuple_data_);
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    type::Value value = (type::ValueFactory::GetNullValueByType(
        tuple_schema_->GetType(column_itr)));
    SetValue(column_itr, value, nullptr);
  }
}

void Tuple::SetAllZeros() {
  PL_ASSERT(tuple_schema_);
  PL_ASSERT(tuple_data_);
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    type::Value value(type::ValueFactory::GetZeroValueByType(
        tuple_schema_->GetType(column_itr)));
    SetValue(column_itr, value, nullptr);
  }
}

int Tuple::Compare(const Tuple &other) const {
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    type::Value lhs = (GetValue(column_itr));
    type::Value rhs = (other.GetValue(column_itr));
    if (lhs.CompareGreaterThan(rhs) == CmpBool::TRUE) {
      return 1;
    }
    if (lhs.CompareLessThan(rhs) == CmpBool::TRUE) {
      return -1;
    }
  }

  return 0;
}

int Tuple::Compare(const Tuple &other,
                   const std::vector<oid_t> &columns) const {
  for (auto column_itr : columns) {
    type::Value lhs = (GetValue(column_itr));
    type::Value rhs = (other.GetValue(column_itr));
    if (lhs.CompareGreaterThan(rhs) == CmpBool::TRUE) {
      return 1;
    }
    if (lhs.CompareLessThan(rhs) == CmpBool::TRUE) {
      return -1;
    }
  }

  return 0;
}

size_t Tuple::HashCode(size_t seed) const {
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    type::Value value = (GetValue(column_itr));
    value.HashCombine(seed);
  }

  return seed;
}

void Tuple::MoveToTuple(const void *address) {
  PL_ASSERT(tuple_schema_);
  tuple_data_ = reinterpret_cast<char *>(const_cast<void *>(address));
}

size_t Tuple::HashCode() const {
  size_t seed = 0;
  return HashCode(seed);
}

char *Tuple::GetDataPtr(const oid_t column_id) {
  PL_ASSERT(tuple_schema_);
  PL_ASSERT(tuple_data_);
  return &tuple_data_[tuple_schema_->GetOffset(column_id)];
}

const char *Tuple::GetDataPtr(const oid_t column_id) const {
  PL_ASSERT(tuple_schema_);
  PL_ASSERT(tuple_data_);
  return &tuple_data_[tuple_schema_->GetOffset(column_id)];
}

const std::string Tuple::GetInfo() const {
  std::stringstream os;

  oid_t column_count = GetColumnCount();
  bool first = true;
  os << "(";
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(column_itr)) {
      os << "<NULL>";
    } else {
      type::Value val = (GetValue(column_itr));
      os << val.ToString();
    }
  }
  os << ")";
  return os.str();
}

}  // namespace storage
}  // namespace peloton
