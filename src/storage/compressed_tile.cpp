//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compressed_tile.cpp
//
// Identification: src/storage/compressed_tile.cpp
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
#include "type/types.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/storage_manager.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"
#include "storage/tuple_iterator.h"
#include "storage/compressed_tile.h"
#include "container/cuckoo_map.h"

namespace peloton {
namespace storage {

bool CompareLessThanBool(type::Value left, type::Value right) {
  return left.CompareLessThan(right);
}

// Constructor: Call base constructor and set is_compressed to false.
CompressedTile::CompressedTile(BackendType backend_type,
                               TileGroupHeader *tile_header,
                               const catalog::Schema &tuple_schema,
                               TileGroup *tile_group, int tuple_count)
    : Tile(backend_type, tile_header, tuple_schema, tile_group, tuple_count) {
  is_compressed = false;  // tile is currently uncompressed.
  compressed_columns_count = 0;
}

CompressedTile::~CompressedTile() {}

// CompressTile
// @brief: This function identifies the columns that can be compressed and
//         determines the smallest compression type that can be used.
//         The compression scheme for each type:
//           1. INTEGER Types other than TINYINT - Find the median and store
//              it as the base_value, store all the remaining values in types
//              of smaller sizes.
//           2. DECIMAL - Same as INTEGER Types, additionally store the
//              maximum exponent.
//           3. VARCHAR - Deduplicate the VARCHAR columns using a Cuckoo Hash.
//        For each compressed column, store the metadata for retrieving the
//        original values.

void CompressedTile::CompressTile(Tile *tile) {
  auto allocated_tuple_count = tile->GetAllocatedTupleCount();
  std::vector<type::Value> new_column_values(allocated_tuple_count);
  std::vector<std::vector<type::Value>> new_columns(column_count);
  std::vector<catalog::Column> columns;
  type::Value max_exponent_count;
  std::vector<type::Value> column_values;
  auto tile_schema = tile->GetSchema();
  auto compression_type = type::Type::TINYINT;

  for (oid_t i = 0; i < column_count; i++) {
    type::Value base_value;
    LOG_TRACE("Compressing Column %s",
              tile_schema->GetColumn(i).GetName().c_str());

    switch (tile_schema->GetType(i)) {
      case type::Type::SMALLINT:
      case type::Type::INTEGER:
      case type::Type::PARAMETER_OFFSET:
      case type::Type::BIGINT:
        column_values = GetIntegerColumnValues(tile, i);
        new_column_values = CompressColumn(tile, i, column_values, base_value,
                                           compression_type);
        if (new_column_values.size() != 0) {
          compressed_columns_count += 1;

          LOG_TRACE("Compressed %s to %s",
                    peloton::TypeIdToString(tile_schema->GetType(i)).c_str(),
                    peloton::TypeIdToString(compression_type).c_str());

          SetCompressedMapValue(i, compression_type, base_value);

        } else {
          LOG_TRACE("Unable to compress %s ",
                    peloton::TypeIdToString(tile_schema->GetType(i)).c_str());
        }
        new_columns[i] = new_column_values;
        break;
      case type::Type::DECIMAL:
        max_exponent_count = GetMaxExponentLength(tile, i);
        column_values = ConvertDecimalColumn(tile, i, max_exponent_count);
        new_column_values = CompressColumn(tile, i, column_values, base_value,
                                           compression_type);
        base_value =
            base_value.CastAs(type::Type::DECIMAL).Divide(max_exponent_count);

        if (new_column_values.size() != 0) {
          compressed_columns_count += 1;

          LOG_TRACE("Compressed %s to %s",
                    peloton::TypeIdToString(tile_schema->GetType(i)).c_str(),
                    peloton::TypeIdToString(compression_type).c_str());
          SetCompressedMapValue(i, compression_type, base_value);
          SetExponentMapValue(i, max_exponent_count);

        } else {
          LOG_TRACE("Unable to compress %s ",
                    peloton::TypeIdToString(tile_schema->GetType(i)).c_str());
        }
        new_columns[i] = new_column_values;
        break;
      case type::Type::VARCHAR:
        LOG_INFO("dictionary");
        new_column_values = CompressCharColumn(tile, i);
        if (new_column_values.size() == 0) {
          LOG_INFO("No deduplicate is needed");
        } else {
          new_columns[i] = new_column_values;
          type::Type::TypeId type_id = GetCompressedType(new_column_values[0]);
          SetCompressedMapValue(i, type_id,
                                type::ValueFactory::GetVarcharValue(""));
          compressed_columns_count += 1;
        }
        break;
      default:
        LOG_TRACE("Unable to compress %s ",
                  peloton::TypeIdToString(tile_schema->GetType(i)).c_str());
    }
  }

  if (compressed_columns_count != 0) {
    for (oid_t i = 0; i < column_count; i++) {
      // Get Column Info
      auto tile_column = tile_schema->GetColumn(i);
      // Individual Column info
      auto column_type = tile_column.GetType();
      auto column_type_size = tile_column.GetLength();
      auto column_name = tile_column.GetName();
      bool column_is_inlined = tile_column.IsInlined();

      if (new_columns[i].size() == 0) {
        catalog::Column column(column_type, column_type_size, column_name,
                               column_is_inlined);
        columns.push_back(column);
      } else {
        type::Type::TypeId new_column_type = GetCompressedType(i);
        catalog::Column column(new_column_type,
                               type::Type::GetTypeSize(new_column_type),
                               column_name, column_is_inlined);
        columns.push_back(column);
      }
      LOG_TRACE("column:%d", i);
    }

    auto &storage_manager = storage::StorageManager::GetInstance();

    LOG_TRACE("Size of the tile before compression: %zu", tile_size);

    // Reclaim data from old schema
    storage_manager.Release(backend_type, data);
    data = NULL;

    schema = catalog::Schema(columns);
    tuple_length = schema.GetLength();
    tile_size = num_tuple_slots * tuple_length;

    LOG_TRACE("Size of the tile after compression: %zu", tile_size);

    data = reinterpret_cast<char *>(
        storage_manager.Allocate(backend_type, tile_size));

    PL_ASSERT(data != NULL);

    // zero out the data
    PL_MEMSET(data, 0, tile_size);

    for (oid_t i = 0; i < column_count; i++) {
      bool is_inlined = schema.IsInlined(i);
      size_t new_column_offset = schema.GetOffset(i);

      if (GetCompressedType(i) == type::Type::INVALID) {
        size_t old_column_offset = tile_schema->GetOffset(i);
        auto old_column_type = tile_schema->GetColumn(i).GetType();

        for (oid_t j = 0; j < num_tuple_slots; j++) {
          type::Value old_value = tile->GetValueFast(
              j, old_column_offset, old_column_type, is_inlined);
          Tile::SetValueFast(old_value, j, new_column_offset, old_column_type,
                             is_inlined);
        }

      } else {
        auto new_column_type = schema.GetColumn(i).GetType();

        for (oid_t j = 0; j < num_tuple_slots; j++) {
          Tile::SetValueFast(new_columns[i][j], j, new_column_offset,
                             new_column_type, is_inlined);
        }
      }
    }

    for (oid_t i = 0; i < column_count; i++) {
      column_offset_map[schema.GetOffset(i)] = i;
    }

    is_compressed = true;
  }
}

/*
ConvertDecimalColumn

@brief : Returns the decimal column as a vector of integer values, by
         multiplying each of the decimal values in a column with the value of
         the exponent returned by the GetMaxExponentLength() function
*/

std::vector<type::Value> CompressedTile::ConvertDecimalColumn(
    Tile *tile, oid_t column_id, type::Value exponent) {
  oid_t num_tuples = tile->GetAllocatedTupleCount();
  auto tile_schema = tile->GetSchema();
  bool is_inlined = tile_schema->IsInlined(column_id);
  size_t column_offset = tile_schema->GetOffset(column_id);
  auto column_type = tile_schema->GetType(column_id);
  type::Value decimal_value;
  std::vector<type::Value> values(num_tuples);

  for (oid_t i = 0; i < num_tuples; i++) {
    decimal_value =
        tile->GetValueFast(i, column_offset, column_type, is_inlined);
    values[i] = decimal_value.Multiply(exponent);
  }
  return values;
}

/*
GetIntegerColumnValues

@brief : Returns the integer column values of the column as a vector
*/

std::vector<type::Value> CompressedTile::GetIntegerColumnValues(
    Tile *tile, oid_t column_id) {
  oid_t num_tuples = tile->GetAllocatedTupleCount();
  auto tile_schema = tile->GetSchema();
  bool is_inlined = tile_schema->IsInlined(column_id);
  size_t column_offset = tile_schema->GetOffset(column_id);
  auto column_type = tile_schema->GetType(column_id);

  std::vector<type::Value> column_values(num_tuples);

  for (oid_t i = 0; i < num_tuples; i++) {
    column_values[i] =
        tile->GetValueFast(i, column_offset, column_type, is_inlined);
  }
  return column_values;
}

/*
GetMaxExponentLength()

@brief : Returns the exponent needed to convert the entire decimal column to
         an integer column.
*/

type::Value CompressedTile::GetMaxExponentLength(Tile *tile, oid_t column_id) {
  oid_t num_tuples = tile->GetAllocatedTupleCount();
  auto tile_schema = tile->GetSchema();
  bool is_inlined = tile_schema->IsInlined(column_id);
  size_t column_offset = tile_schema->GetOffset(column_id);
  auto column_type = tile_schema->GetType(column_id);

  type::Value decimal_value;
  type::Value increment = type::ValueFactory::GetTinyIntValue(10);

  type::Value current_max = type::ValueFactory::GetBigIntValue(1);

  for (oid_t i = 0; i < num_tuples; i++) {
    decimal_value =
        tile->GetValueFast(i, column_offset, column_type, is_inlined);

    type::Value new_value = decimal_value.Multiply(current_max);

    while (new_value.CompareNotEquals(new_value.CastAs(type::Type::BIGINT))) {
      current_max = current_max.Multiply(increment);
      new_value = new_value.Multiply(current_max);
    }
  }

  return current_max;
}

/*
CompressColumn()

@brief : The following function is used to compress all columns of a datatable
         except VARCHAR. It sorts the column and sets the median value as the
base value for the delta encoding. The remainig values are stored as offsets
from this base value.

@return :The modified values of the column i.e. the offsets of the values of
         the columns in a vector.
*/

std::vector<type::Value> CompressedTile::CompressColumn(
    Tile *tile, oid_t column_id, std::vector<type::Value> column_values,
    type::Value &base_value, type::Type::TypeId &compression_type) {
  oid_t num_tuples = tile->GetAllocatedTupleCount();
  auto tile_schema = tile->GetSchema();
  auto column_type = tile_schema->GetType(column_id);
  std::vector<type::Value> actual_values(column_values);
  compression_type = type::Type::TINYINT;

  std::sort(column_values.begin(), column_values.end(), CompareLessThanBool);

  base_value = column_values[num_tuples / 2];
  type::Value maxDiff = column_values[num_tuples - 1].Subtract(base_value);
  type::Value minDiff = column_values[0].Subtract(base_value);

  std::vector<type::Value> modified_values(num_tuples);

  while (true) {
    type::Value maxValue = type::Type::GetMaxValue(compression_type);
    type::Value minValue = type::Type::GetMinValue(compression_type);

    if ((maxDiff.CompareLessThanEquals(maxValue) == type::CMP_TRUE) &&
        (minDiff.CompareGreaterThanEquals(minValue) == type::CMP_TRUE)) {
      // Found a  suitable compression type.
      for (oid_t k = 0; k < num_tuples; k++) {
        modified_values[k] =
            actual_values[k].Subtract(base_value).CastAs(compression_type);
      }
      break;
    } else {
      if (type::Type::GetTypeSize(column_type) ==
          type::Type::GetTypeSize(static_cast<type::Type::TypeId>(
              static_cast<int>(compression_type) + 1))) {
        modified_values.resize(0);
        break;
      }

      compression_type = static_cast<type::Type::TypeId>(
          static_cast<int>(compression_type) + 1);
    }
  }
  return modified_values;
}

// compression for varchar
std::vector<type::Value> CompressedTile::CompressCharColumn(Tile *tile,
                                                            oid_t column_id) {
  oid_t num_tuples = tile->GetAllocatedTupleCount();
  auto tile_schema = tile->GetSchema();
  bool is_inlined = tile_schema->IsInlined(column_id);
  size_t column_offset = tile_schema->GetOffset(column_id);
  auto column_type = tile_schema->GetType(column_id);
  
  std::vector< type::Value > encoded_values(num_tuples);
  std::map< std::string, int > encoder;
  std::vector< type::Value> decoder;
  oid_t num_unique_words = 0;
  std::map< std::string, int>::iterator it;

  for (oid_t i = 0; i < num_tuples; i++) {
    
    type::Value val =
        tile->GetValueFast(i, column_offset, column_type, is_inlined);
    std::string string_val = val.ToString();
     it = encoder.find(string_val);
    
    if (it == encoder.end()) {
      encoder[string_val] = num_unique_words;
      type::Value encoded_value = type::ValueFactory::GetSmallIntValue(num_unique_words);
      decoder.push_back(type::ValueFactory::GetVarcharValue(string_val));
      encoded_values[i] = encoded_value;
      num_unique_words++;
    } else {
      encoded_values[i] = type::ValueFactory::GetSmallIntValue(encoder[string_val]);
    }
  }

  LOG_TRACE("Number of unique words in column : %d", (int)num_unique_words);
  
  if (static_cast<double>(num_unique_words) > (0.75 * static_cast<double>(num_tuples))) {
    encoded_values.clear();
  } else {
    SetDecoderMapValue(column_id, decoder);
  }
  return encoded_values;
}

/*
InsertTuple()

@brief : The following function prevents insertion of tuples in compressed
tiles.
*/

void CompressedTile::InsertTuple(const oid_t tuple_offset, Tuple *tuple) {
  if (IsCompressed()) {
    LOG_TRACE("Peloton does not support insert into compressed tiles.");
    PL_ASSERT(false);
  } else {
    Tile::InsertTuple(tuple_offset, tuple);
  }
}

/*
GetValue()

@brief : The following function checks if the tile is compressed. If yes, return
the uncompressed value, else return the deserializedValue.

@return : Actual Value(uncompressed) of the tuple at tuple offset.
*/

type::Value CompressedTile::GetValue(const oid_t tuple_offset,
                                     const oid_t column_id) {
  type::Value deserializedValue = Tile::GetValue(tuple_offset, column_id);

  if (!IsCompressed()) {
    return deserializedValue;
  }

  if (GetCompressedType(column_id) == type::Type::INVALID) {
    return deserializedValue;
  }

  type::Type::TypeId compressed_type = GetCompressedType(column_id);

  PL_ASSERT(compressed_type != type::Type::INVALID);

  type::Value original = GetUncompressedValue(column_id, deserializedValue);

  return original;
}

/*
GetValueFast()

@brief : Faster way to get value by amortizing schema lookups.

@return :  Actual Value(uncompressed) of the tuple at tuple offset.
*/

type::Value CompressedTile::GetValueFast(const oid_t tuple_offset,
                                         const size_t column_offset,
                                         const type::Type::TypeId column_type,
                                         const bool is_inlined) {
  type::Value deserializedValue =
      Tile::GetValueFast(tuple_offset, column_offset, column_type, is_inlined);

  if (!IsCompressed()) {
    return deserializedValue;
  }

  oid_t column_id = GetColumnFromOffset(column_offset);

  PL_ASSERT(column_id < column_count);

  if (GetCompressedType(column_id) != type::Type::INVALID) {
    return GetUncompressedValue(column_id, deserializedValue);
  }

  return deserializedValue;
}

/*
NOTE: We don't allow setting values in compressed tiles.
*/

void CompressedTile::SetValue(const type::Value &value,
                              const oid_t tuple_offset, const oid_t column_id) {
  if ((IsCompressed()) &&
      (GetCompressedType(column_id) != type::Type::INVALID)) {
    LOG_TRACE("Peloton does not support SetValue on a CompressedTile");
    PL_ASSERT(false);
  }

  Tile::SetValue(value, tuple_offset, column_id);
}

void CompressedTile::SetValueFast(const type::Value &value,
                                  const oid_t tuple_offset,
                                  const size_t column_offset,
                                  const bool is_inlined,
                                  const size_t column_length) {
  oid_t column_id = GetColumnFromOffset(column_offset);

  PL_ASSERT(column_id < column_count);

  if (IsCompressed()) {
    if (GetCompressedType(column_id) != type::Type::INVALID) {
      LOG_TRACE("Peloton does not support SetValueFast on CompressedTile");
      PL_ASSERT(false);
    }
  }

  Tile::SetValueFast(value, tuple_offset, column_offset, is_inlined,
                     column_length);
}

type::Value CompressedTile::GetUncompressedVarcharValue(
    oid_t column_id, type::Value compressed_value) {
  int offset;
  if (compressed_value.GetTypeId() == type::Type::TINYINT)
    offset = (int32_t)compressed_value.GetAs<int8_t>();
  else
    offset = (int32_t)compressed_value.GetAs<int16_t>();

  type::Value value;
  value = decoder_map[column_id].at(offset);

  return value;
}

type::Value CompressedTile::GetUncompressedValue(oid_t column_id,
                                                 type::Value compressed_value) {
  if (compressed_column_map.find(column_id) != compressed_column_map.end()) {
    type::Value base_value = compressed_column_map[column_id].second;

    if (base_value.GetTypeId() == type::Type::VARCHAR) {
      return GetUncompressedVarcharValue(column_id, compressed_value);
    }

    if (base_value.GetTypeId() == type::Type::DECIMAL) {
      compressed_value = compressed_value.CastAs(type::Type::DECIMAL)
                             .Divide(exponent_column_map[column_id]);
    }

    return base_value.Add(compressed_value);
  }

  return type::Value();
}
}
}
