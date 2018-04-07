//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile.cpp
//
// Identification: src/storage/tile.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <sstream>

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/serializer.h"
#include "common/internal_types.h"
#include "type/ephemeral_pool.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/backend_manager.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"
#include "storage/tuple_iterator.h"

namespace peloton {
namespace storage {

Tile::Tile(BackendType backend_type, TileGroupHeader *tile_header,
           const catalog::Schema &tuple_schema, TileGroup *tile_group,
           int tuple_count)
    : database_id(INVALID_OID),
      table_id(INVALID_OID),
      tile_group_id(INVALID_OID),
      tile_id(INVALID_OID),
      backend_type(backend_type),
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
      is_dict_encoded(false)  {
  PELOTON_ASSERT(tuple_count > 0);

  tile_size = tuple_count * tuple_length;

  // allocate tuple storage space for inlined data
  // auto &storage_manager = storage::StorageManager::GetInstance();
  // data = reinterpret_cast<char *>(
  // storage_manager.Allocate(backend_type, tile_size));

  data = new char[tile_size];
  PELOTON_ASSERT(data != NULL);

  // zero out the data
  PELOTON_MEMSET(data, 0, tile_size);

  // allocate pool for blob storage if schema not inlined
  // if (schema.IsInlined() == false) {
  pool = new type::EphemeralPool();
  //}
}

Tile::~Tile() {
  // reclaim the tile memory (INLINED data)
  // auto &storage_manager = storage::StorageManager::GetInstance();
  // storage_manager.Release(backend_type, data);

  delete[] data;
  data = NULL;

  // reclaim the tile memory (UNINLINED data)
  // if (schema.IsInlined() == false) {
  delete pool;
  //}
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
void Tile::InsertTuple(const oid_t tuple_offset, Tuple *tuple) {
  PELOTON_ASSERT(tuple_offset < GetAllocatedTupleCount());

  // Find slot location
  char *location = tuple_offset * tuple_length + data;

  // Copy over the tuple data into the tuple slot in the tile
  PELOTON_MEMCPY(location, tuple->tuple_data_, tuple_length);
}

/**
 * Returns value present at slot
 */
// column id is a 0-based column number
type::Value Tile::GetValue(const oid_t tuple_offset, const oid_t column_id) {
  PELOTON_ASSERT(tuple_offset < GetAllocatedTupleCount());
  PELOTON_ASSERT(column_id < schema.GetColumnCount());

  const type::TypeId column_type = schema.GetType(column_id);

  const char *tuple_location = GetTupleLocation(tuple_offset);
  const char *field_location = tuple_location + schema.GetOffset(column_id);
  const bool is_inlined = schema.IsInlined(column_id);
  // add condition to handle encoded data
	if (is_dict_encoded && dict_encoded_columns.count(column_id) > 0) {
		auto idx_val = type::Value::DeserializeFrom(field_location, column_type, is_inlined);
		uint8_t idx = idx_val.GetData()[0];
		auto true_str = element_array[idx].c_str();
		return type::Value::DeserializeFrom(true_str, column_type, is_inlined);
	} else {
		return type::Value::DeserializeFrom(field_location, column_type, is_inlined);
	}
}

/*
 * Faster way to get value
 * By amortizing schema lookups
 */
// column offset is the actual offset of the column within the tuple slot
type::Value Tile::GetValueFast(const oid_t tuple_offset,
                               const size_t column_offset,
                               const type::TypeId column_type,
                               const bool is_inlined) {
  PELOTON_ASSERT(tuple_offset < GetAllocatedTupleCount());
  PELOTON_ASSERT(column_offset < schema.GetLength());

  const char *tuple_location = GetTupleLocation(tuple_offset);
  const char *field_location = tuple_location + column_offset;

  return type::Value::DeserializeFrom(field_location, column_type, is_inlined);
}

/**
 * Sets value at tuple slot.
 */
// column id is a 0-based column number
void Tile::SetValue(const type::Value &value, const oid_t tuple_offset,
                    const oid_t column_id) {
  PELOTON_ASSERT(tuple_offset < num_tuple_slots);
  PELOTON_ASSERT(column_id < schema.GetColumnCount());

  char *tuple_location = GetTupleLocation(tuple_offset);
  char *field_location = tuple_location + schema.GetOffset(column_id);
  const bool is_inlined = schema.IsInlined(column_id);
  // size_t column_length = schema.GetAppropriateLength(column_id);

  // const bool is_in_bytes = false;
  PELOTON_ASSERT(pool != nullptr);
  // Cast the value if the type is different from column type
  const type::TypeId col_type = schema.GetType(column_id);
  if (value.GetTypeId() == col_type) {
    value.SerializeTo(field_location, is_inlined, pool);
  } else {
    type::Value casted_value = value.CastAs(col_type);
    casted_value.SerializeTo(field_location, is_inlined, pool);
  }
}

/*
 * Faster way to set value
 * By amortizing schema lookups
 */
// column offset is the actual offset of the column within the tuple slot
void Tile::SetValueFast(const type::Value &value, const oid_t tuple_offset,
                        const size_t column_offset, const bool is_inlined,
                        UNUSED_ATTRIBUTE const size_t column_length) {
  PELOTON_ASSERT(tuple_offset < num_tuple_slots);
  PELOTON_ASSERT(column_offset < schema.GetLength());

  char *tuple_location = GetTupleLocation(tuple_offset);
  char *field_location = tuple_location + column_offset;

  // const bool is_in_bytes = false;
  PELOTON_ASSERT(pool != nullptr);
  value.SerializeTo(field_location, is_inlined, pool);
}

Tile *Tile::CopyTile(BackendType backend_type) {
  auto schema = GetSchema();
  bool tile_columns_inlined = schema->IsInlined();
  auto allocated_tuple_count = GetAllocatedTupleCount();

  // Create a shallow copy of the old tile
  TileGroupHeader *new_header = GetHeader();
  Tile *new_tile = TileFactory::GetTile(
      backend_type, INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
      new_header, *schema, tile_group, allocated_tuple_count);

  PELOTON_MEMCPY(static_cast<void *>(new_tile->data), static_cast<void *>(data),
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
        type::Value val = (new_tile->GetValue(tuple_itr, uninlined_col_offset));
        new_tile->SetValue(val, tuple_itr, uninlined_col_offset);
      }
    }
  }

  return new_tile;
}

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

const std::string Tile::GetInfo() const {
  std::ostringstream os;

  os << "TILE[#" << tile_id << "]" << std::endl;
  os << "Database[" << database_id << "] // ";
  os << "Table[" << table_id << "] // ";
  os << "TileGroup[" << tile_group_id << "]" << std::endl;

  // Tuples
  os << GETINFO_SINGLE_LINE << std::endl;

  TupleIterator tile_itr(this);
  Tuple tuple(&schema);

  int tupleCtr = 0;
  while (tile_itr.Next(tuple)) {
    if (tupleCtr > 0) os << std::endl;
    os << std::setfill('0') << std::setw(TUPLE_ID_WIDTH) << tupleCtr++ << ": ";
    os << tuple;
  }
  tuple.SetNull();
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
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

  PELOTON_ASSERT(written_count == num_tuples);

  // Length prefix is non-inclusive
  int32_t sz = static_cast<int32_t>(output.Position() - pos - sizeof(int32_t));
  PELOTON_ASSERT(sz > 0);
  output.WriteIntAt(pos, sz);

  return true;
}

bool Tile::SerializeHeaderTo(SerializeOutput &output) {
  std::size_t start;

  // Use the cache if possible
  if (column_header != NULL) {
    PELOTON_ASSERT(column_header_size != INVALID_OID);
    output.WriteBytes(column_header, column_header_size);
    return true;
  }

  PELOTON_ASSERT(column_header_size == INVALID_OID);

  // Skip header position
  start = output.Position();
  output.WriteInt(-1);

  // Status code
  output.WriteByte(-128);

  // Column counts as a short
  output.WriteShort(static_cast<int16_t>(column_count));

  // Write an array of column types as bytes
  for (oid_t column_itr = 0; column_itr < column_count; ++column_itr) {
    type::TypeId type = schema.GetType(column_itr);
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
    PELOTON_ASSERT(length >= 0);

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
  PELOTON_MEMCPY(column_header, static_cast<const char *>(output.Data()) + start,
            column_header_size);

  return true;
}

//  Serialized only the tuples specified, along with header.
bool Tile::SerializeTuplesTo(SerializeOutput &output, Tuple *tuples,
                             int num_tuples) {
  std::size_t pos = output.Position();
  output.WriteInt(-1);

  PELOTON_ASSERT(!tuples[0].IsNull());

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
void Tile::DeserializeTuplesFrom(SerializeInput &input,
                                 type::AbstractPool *pool) {
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
  PELOTON_ASSERT(column_count > 0);

  // Store the following information so that we can provide them to the user on
  // failure
  type::TypeId types[column_count];
  std::vector<std::string> names;

  // Skip the column types
  for (oid_t column_itr = 0; column_itr < column_count; ++column_itr) {
    types[column_itr] = (type::TypeId)input.ReadEnumInSingleByte();
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
              << ", type = " << static_cast<int>(types[column_itr]) << std::endl;
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
void Tile::DeserializeTuplesFromWithoutHeader(SerializeInput &input,
                                              type::AbstractPool *pool) {
  oid_t tuple_count = input.ReadInt();
  PELOTON_ASSERT(tuple_count > 0);

  // First, check if we have required space
  PELOTON_ASSERT(tuple_count <= num_tuple_slots);
  storage::Tuple *temp_tuple = new storage::Tuple(&schema, true);

  for (oid_t tuple_itr = 0; tuple_itr < tuple_count; ++tuple_itr) {
    temp_tuple->Move(GetTupleLocation(tuple_itr));
    temp_tuple->DeserializeFrom(input, pool);
    // TRACE("Loaded new tuple #%02d\n%s", tuple_itr,
    // temp_target1.debug(Name()).c_str());
  }
}

// active tuple slots
oid_t Tile::GetActiveTupleCount() const {
  // For normal tiles
  if (tile_group_header != nullptr) {
    return tile_group_header->GetCurrentNextTupleSlot();
  }
  // For temp tiles
  else {
    return num_tuple_slots;
  }
}

void Tile::Sync() {
  // Sync the tile data
  // auto &storage_manager = storage::StorageManager::GetInstance();
  // storage_manager.Sync(backend_type, data, tile_size);
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

//===--------------------------------------------------------------------===//
// Dictionary Encoding
// ===--------------------------------------------------------------------===//

// need to take care of tile schema, after encoding, encoded columns have type
// VARCHAR or VARBINARY, but actually save TINYINT values
void Tile::DictEncode() {
	LOG_INFO("dictionary encode, database_id: %d, table_id: %d, tile_group_id: %d"
					", tile_id: %d", database_id, table_id, tile_group_id, tile_id);

	// need to modify tuple length, data
	size_t new_tuple_length = 0;
	for(oid_t i = 0; i < column_count; i++) {
//		LOG_INFO("encoding column %s", schema.GetColumn(i).column_name.c_str());
		auto column_type = schema.GetType(i);
		auto column_length = schema.GetColumn(i).GetLength();
		if (column_type == type::TypeId::VARCHAR ||
				column_type == type::TypeId::VARBINARY) {
			// compress to ... what size? Let's assume a tinyint (1 byte)
			new_tuple_length += type::Type::GetTypeSize(type::TypeId::TINYINT);
		} else {
			new_tuple_length += column_length;
		}
	}

	if (new_tuple_length == tuple_length) {
		// no compression, return
		return;
	}
	size_t new_tile_size = num_tuple_slots * new_tuple_length;
//	auto old_data = data;
//	data = new char[new_tile_size];

	// use a 2d vector to save the data
	std::vector<std::vector<type::Value>> new_data_vector(column_count);
	std::vector<type::TypeId> column_types(column_count);
	std::vector<bool> column_inline(column_count);
	for (oid_t i = 0; i < column_count; i++) {
		auto column_type = schema.GetType(i);
		column_types[i] = column_type;
		auto column_is_inlined = schema.GetColumn(i).IsInlined();
		column_inline[i] = column_is_inlined;
		// if it is inlined, no need to compress!
		if ((column_type == type::TypeId::VARCHAR ||
				column_type == type::TypeId::VARBINARY) && column_is_inlined) {
			LOG_INFO("encoding column %s", schema.GetColumn(i).column_name.c_str());
			column_types[i] = type::TypeId::TINYINT;
			dict_encoded_columns.insert(i);
			// to for tuple offset
			for (oid_t to = 0; to < num_tuple_slots; to++) {
				auto curr_val = GetValueFast(to, i, column_type, column_is_inlined);
				std::string curr_val_str(curr_val.GetData());
				// assume the idx take 1 byte
				char idx[1];
				if (dict.count(curr_val_str) == 0) {
					element_array.push_back(curr_val_str);
					idx[0] = element_array.size() - 1;
					dict.emplace(curr_val_str, idx);
				} else {
					idx[0] = dict[curr_val_str];
				}
				// many constructor of Value is private, so use
				// DeserializeFrom to construct the idx Value
				type::Value idx_val(type::Value::DeserializeFrom(idx, type::TypeId::TINYINT, true));
				new_data_vector[i].push_back(idx_val);
			}
		} else {
			for (oid_t to = 0; to < num_tuple_slots; to++) {
				new_data_vector[i].push_back(GetValueFast(to, i, column_type, column_is_inlined));
			}
		}
	}

	// now we have all data in new_data_vector
	// put new data into storage space
	delete[] data;
	data = new char[new_tile_size];
	for (oid_t i = 0; i < column_count; i++) {
		for (oid_t to = 0; to < num_tuple_slots; to++) {
			SetValueFast(new_data_vector[i][to], to, i, column_inline[i],
					type::Type::GetTypeSize(column_types[i]));
		}
	}
	is_dict_encoded = true;
}


}  // namespace storage
}  // namespace peloton
