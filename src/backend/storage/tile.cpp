//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile.cpp
//
// Identification: src/backend/storage/tile.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/tile.h"

#include <cassert>
#include <cstdio>

#include <sstream>

#include "backend/catalog/schema.h"
#include "backend/common/exception.h"
#include "backend/common/pool.h"
#include "backend/common/serializer.h"
#include "backend/common/types.h"
#include "backend/storage/tuple_iterator.h"
#include "backend/storage/tuple.h"
#include "backend/storage/backend.h"

namespace peloton {
namespace storage {

Tile::Tile(TileGroupHeader *tile_header,
           const catalog::Schema &tuple_schema,
           TileGroup *tile_group,
           int tuple_count)
: database_id(INVALID_OID),
  table_id(INVALID_OID),
  tile_group_id(INVALID_OID),
  tile_id(INVALID_OID),
  schema(tuple_schema),
  data(NULL),
  tile_group(tile_group),
  pool(NULL),
  num_tuple_slots(tuple_count),
  column_count(tuple_schema.GetColumnCount()),
  tuple_length(tuple_schema.GetLength()),
  uninlined_data_size(0),
  column_header(NULL),
  column_header_size(INVALID_OID),
  tile_group_header(tile_header),
  ref_count(BASE_REF_COUNT){
  assert(tuple_count > 0);

  tile_size = tuple_count * tuple_length;

  // allocate tuple storage space for inlined data
  auto backend = storage::Backend::GetInstance();
  data = (char *)backend.Allocate(tile_size);
  assert(data != NULL);

  // initialize it
  std::memset(data, 0, tile_size);

  // allocate pool for blob storage if schema not inlined
  if (schema.IsInlined() == false) pool = new VarlenPool();
}

Tile::~Tile() {
  // reclaim the tile memory (INLINED data)
  auto backend = storage::Backend::GetInstance();
  backend.Free(data);
  data = NULL;

  // reclaim the tile memory (UNINLINED data)
  if (schema.IsInlined() == false) delete pool;
  pool = NULL;

  // clear any cached column headers
  if (column_header) delete column_header;
  column_header = NULL;

}

//===--------------------------------------------------------------------===//
// Tuples
//===--------------------------------------------------------------------===//

/**
 * Insert tuple at slot
 * NOTE : No checks, must be at valid slot.
 */
void Tile::InsertTuple(const oid_t tuple_slot_id, Tuple *tuple) {
  // Find slot location
  char *location = tuple_slot_id * tuple_length + data;

  std::memcpy(location, tuple->tuple_data, tuple_length);
}

/**
 * Returns tuple present at slot
 * NOTE : No checks, must be at valid slot and must exist.
 */
Tuple *Tile::GetTuple(const oid_t tuple_slot_id) {
  storage::Tuple *tuple = new storage::Tuple(&schema, true);

  tuple->Copy(GetTupleLocation(tuple_slot_id), pool);

  return tuple;
}

/**
 * Returns value present at slot
 */
Value Tile::GetValue(const oid_t tuple_slot_id, const oid_t column_id) {
  assert(tuple_slot_id < GetAllocatedTupleCount());
  assert(column_id < schema.GetColumnCount());

  const ValueType column_type = schema.GetType(column_id);

  const char *tuple_location = GetTupleLocation(tuple_slot_id);
  const char *field_location = tuple_location + schema.GetOffset(column_id);
  const bool is_inlined = schema.IsInlined(column_id);

  return Value::InitFromTupleStorage(field_location, column_type, is_inlined);
}

// Faster way to access value
// By amortizing schema lookups
Value Tile::GetValueFast(const oid_t tuple_slot_id,
                         const size_t column_offset,
                         const ValueType column_type,
                         const bool is_inlined) {

  const char *tuple_location = GetTupleLocation(tuple_slot_id);
  const char *field_location = tuple_location + column_offset;

  return Value::InitFromTupleStorage(field_location, column_type, is_inlined);
}

/**
 * Sets value at tuple slot.
 * TODO We might want to write an iterator class to amortize the schema
 * lookups when setting values of entire columns.
 */
void Tile::SetValue(Value value, const oid_t tuple_slot_id,
                    const oid_t column_id) {
  assert(tuple_slot_id < num_tuple_slots);

  char *tuple_location = GetTupleLocation(tuple_slot_id);
  char *field_location = tuple_location + schema.GetOffset(column_id);
  const bool is_inlined = schema.IsInlined(column_id);
  size_t column_length = schema.GetAppropriateLength(column_id);

  // TODO: Not sure about the argument
  const bool is_in_bytes = false;
  value.SerializeToTupleStorageAllocateForObjects(field_location,
                                                  is_inlined,
                                                  column_length,
                                                  is_in_bytes,
                                                  pool);
}

void Tile::SetValueFast(Value value, const oid_t tuple_slot_id,
                        const size_t column_offset,
                        const bool is_inlined,
                        const size_t column_length) {

  char *tuple_location = GetTupleLocation(tuple_slot_id);
  char *field_location = tuple_location + column_offset;

  // TODO: Not sure about the argument
  const bool is_in_bytes = false;
  value.SerializeToTupleStorageAllocateForObjects(field_location,
                                                  is_inlined,
                                                  column_length,
                                                  is_in_bytes,
                                                  pool);
}

Tile *Tile::CopyTile() {
  auto schema = GetSchema();
  bool tile_columns_inlined = schema->IsInlined();
  auto allocated_tuple_count = GetAllocatedTupleCount();

  // Create a shallow copy of the old tile
  TileGroupHeader *new_header = GetHeader();
  Tile *new_tile = TileFactory::GetTile(
      INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID, new_header,
      *schema, tile_group, allocated_tuple_count);

  ::memcpy(static_cast<void *>(new_tile->data), static_cast<void *>(data),
           tile_size);

  // Do a deep copy if some column is uninlined, so that
  // the values in that column point to the new pool
  if (!tile_columns_inlined) {
    auto uninlined_col_cnt = schema->GetUninlinedColumnCount();

    // Go over each uninlined column, making a deep copy
    for (oid_t col_itr = 0; col_itr < uninlined_col_cnt; col_itr++) {
      auto uninlined_col_offset = schema->GetUninlinedColumn(col_itr);

      // Copy the column over to the new tile group
      for (oid_t tuple_itr = 0; tuple_itr < allocated_tuple_count;
          tuple_itr++) {
        auto val = new_tile->GetValue(tuple_itr, uninlined_col_offset);
        new_tile->SetValue(val, tuple_itr, uninlined_col_offset);
      }
    }
  }

  return new_tile;
}

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

// Get a string representation of this tile
std::ostream &operator<<(std::ostream &os, const Tile &tile) {
  os << "\t-----------------------------------------------------------\n";

  os << "\tTILE\n";
  os << "\tCatalog ::"
      << " DB: " << tile.database_id << " Table: " << tile.table_id
      << " Tile Group:  " << tile.tile_group_id << " Tile:  " << tile.tile_id
      << "\n";

  // Columns
  // os << "\t-----------------------------------------------------------\n";
  // os << "\tSCHEMA\n";
  // os << (*tile.schema);

  // Tuples
  os << "\t-----------------------------------------------------------\n";
  os << "\tDATA\n";

  TupleIterator tile_itr(&tile);
  Tuple tuple(&tile.schema);

  std::string last_tuple = "";

  while (tile_itr.Next(tuple)) {
    os << "\t" << tuple;
  }

  os << "\t-----------------------------------------------------------\n";

  tuple.SetNull();

  return os;
}

//===--------------------------------------------------------------------===//
// Serialization/Deserialization
//===--------------------------------------------------------------------===//

bool Tile::SerializeTo(SerializeOutput &output, oid_t num_tuples) {
  /**
   * The table is serialized as:
   *
   * [(int) total size]
   * [(int) header size] [num columns] [column types] [column names]
   * [(int) num tuples] [tuple data]
   *
   */

  // A placeholder for the total table size written at the end
  std::size_t pos = output.Position();
  output.WriteInt(-1);

  // Serialize the header
  if (!SerializeHeaderTo(output)) return false;

  // Active tuple count
  output.WriteInt(static_cast<int>(num_tuples));

  oid_t written_count = 0;
  TupleIterator tile_itr(this);
  Tuple tuple(&schema);

  while (tile_itr.Next(tuple) && written_count < num_tuples) {
    tuple.SerializeTo(output);
    ++written_count;
  }

  tuple.SetNull();

  assert(written_count == num_tuples);

  // Length prefix is non-inclusive
  int32_t sz = static_cast<int32_t>(output.Position() - pos - sizeof(int32_t));
  assert(sz > 0);
  output.WriteIntAt(pos, sz);

  return true;
}

bool Tile::SerializeHeaderTo(SerializeOutput &output) {
  std::size_t start;

  // Use the cache if possible
  if (column_header != NULL) {
    assert(column_header_size != INVALID_OID);
    output.WriteBytes(column_header, column_header_size);
    return true;
  }

  assert(column_header_size == INVALID_OID);

  // Skip header position
  start = output.Position();
  output.WriteInt(-1);

  // Status code
  output.WriteByte(-128);

  // Column counts as a short
  output.WriteShort(static_cast<int16_t>(column_count));

  // Write an array of column types as bytes
  for (oid_t column_itr = 0; column_itr < column_count; ++column_itr) {
    ValueType type = schema.GetType(column_itr);
    output.WriteByte(static_cast<int8_t>(type));
  }

  // Write the array of column names as strings
  // NOTE: strings are ASCII only in metadata (UTF-8 in table storage)
  for (oid_t column_itr = 0; column_itr < column_count; ++column_itr) {
    // Column name: Write (offset, length) for column definition, and string to
    // string table
    const std::string &name = GetColumnName(column_itr);

    // Column names can't be null, so length must be >= 0
    int32_t length = static_cast<int32_t>(name.size());
    assert(length >= 0);

    // this is standard string serialization for voltdb
    output.WriteInt(length);
    output.WriteBytes(name.data(), length);
  }

  // Write the header size which is a non-inclusive int
  size_t Position = output.Position();
  column_header_size = static_cast<int32_t>(Position - start);

  int32_t non_inclusive_header_size =
      static_cast<int32_t>(column_header_size - sizeof(int32_t));
  output.WriteIntAt(start, non_inclusive_header_size);

  // Cache the column header
  column_header = new char[column_header_size];
  memcpy(column_header, static_cast<const char *>(output.Data()) + start,
         column_header_size);

  return true;
}

//  Serialized only the tuples specified, along with header.
bool Tile::SerializeTuplesTo(SerializeOutput &output, Tuple *tuples,
                             int num_tuples) {
  std::size_t pos = output.Position();
  output.WriteInt(-1);

  assert(!tuples[0].IsNull());

  // Serialize the header
  if (!SerializeHeaderTo(output)) return false;

  output.WriteInt(static_cast<int32_t>(num_tuples));
  for (int tuple_itr = 0; tuple_itr < num_tuples; tuple_itr++) {
    tuples[tuple_itr].SerializeTo(output);
  }

  // Length prefix is non-inclusive
  output.WriteIntAt(
      pos, static_cast<int32_t>(output.Position() - pos - sizeof(int32_t)));

  return true;
}

/**
 * Loads only tuple data, not schema, from the serialized tile.
 * Used for initial data loading.
 * @param allow_export if false, export enabled is overriden for this load.
 */
void Tile::DeserializeTuplesFrom(SerializeInputBE &input, VarlenPool *pool) {
  /*
   * Directly receives a Tile buffer.
   * [00 01]   [02 03]   [04 .. 0x]
   * rowstart  colcount  colcount * 1 byte (column types)
   *
   * [0x+1 .. 0y]
   * colcount * strings (column names)
   *
   * [0y+1 0y+2 0y+3 0y+4]
   * rowcount
   *
   * [0y+5 .. end]
   * rowdata
   */

  input.ReadInt();  // rowstart
  input.ReadByte();

  oid_t column_count = input.ReadShort();
  assert(column_count > 0);

  // Store the following information so that we can provide them to the user on
  // failure
  ValueType types[column_count];
  std::vector<std::string> names;

  // Skip the column types
  for (oid_t column_itr = 0; column_itr < column_count; ++column_itr) {
    types[column_itr] = (ValueType)input.ReadEnumInSingleByte();
  }

  // Skip the column names
  for (oid_t column_itr = 0; column_itr < column_count; ++column_itr) {
    names.push_back(input.ReadTextString());
  }

  // Check if the column count matches what the temp table is expecting
  if (column_count != schema.GetColumnCount()) {
    std::stringstream message(std::stringstream::in | std::stringstream::out);

    message << "Column count mismatch. Expecting " << schema.GetColumnCount()
                << ", but " << column_count << " given" << std::endl;
    message << "Expecting the following columns:" << std::endl;
    message << schema.GetColumnCount() << std::endl;
    message << "The following columns are given:" << std::endl;

    for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
      message << "column " << column_itr << ": " << names[column_itr]
                                                          << ", type = " << ValueTypeToString(types[column_itr])
                                                          << std::endl;
    }

    throw SerializationException(message.str());
  }

  // Use the deserialization routine skipping header
  DeserializeTuplesFromWithoutHeader(input, pool);
}

/**
 * Loads only tuple data and assumes there is no schema present.
 * Used for recovery where the schema is not sent.
 * @param allow_export if false, export enabled is overriden for this load.
 */
void Tile::DeserializeTuplesFromWithoutHeader(SerializeInputBE &input,
                                              VarlenPool *pool) {
  oid_t tuple_count = input.ReadInt();
  assert(tuple_count > 0);

  // First, check if we have required space
  assert(tuple_count <= num_tuple_slots);
  storage::Tuple *temp_tuple = new storage::Tuple(&schema, true);

  for (oid_t tuple_itr = 0; tuple_itr < tuple_count; ++tuple_itr) {
    temp_tuple->Move(GetTupleLocation(tuple_itr));
    temp_tuple->DeserializeFrom(input, pool);
    // TRACE("Loaded new tuple #%02d\n%s", tuple_itr,
    // temp_target1.debug(Name()).c_str());
  }
}

void Tile::IncrementRefCount() {
  ++ref_count;
}

void Tile::DecrementRefCount() {

  std::cout << "Dec :: " << this << " [ " << GetRefCount() << " ] \n";

  // DROP tile when ref count reaches 0
  // this returns the value immediately preceding the assignment
  if (ref_count.fetch_sub(1) == BASE_REF_COUNT) {
    delete this;
  }
}

size_t Tile::GetRefCount() const {
  return ref_count;
}


//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

// Compare two tiles (expensive !)
bool Tile::operator==(const Tile &other) const {
  if (!(GetColumnCount() == other.GetColumnCount())) return false;

  if (!(database_id == other.database_id)) return false;

  catalog::Schema other_schema = other.schema;
  if (schema != other_schema) return false;

  TupleIterator tile_itr(this);
  TupleIterator other_tile_itr(&other);

  Tuple tuple(&schema);
  Tuple other_tuple(&other_schema);

  while (tile_itr.Next(tuple)) {
    if (!(other_tile_itr.Next(other_tuple))) return false;

    if (!(tuple == other_tuple)) return false;
  }

  tuple.SetNull();
  other_tuple.SetNull();

  return true;
}

bool Tile::operator!=(const Tile &other) const { return !(*this == other); }

TupleIterator Tile::GetIterator() { return TupleIterator(this); }

// TileStats* Tile::GetTileStats() {
//	return NULL;
//}

}  // End storage namespace
}  // End peloton namespace
