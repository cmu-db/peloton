//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dict_encoded_tile.cpp
//
// Identification: src/include/storage/dict_encoded_tile.cpp
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
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
#include "storage/dict_encoded_tile.h"

namespace peloton {
namespace storage {

DictEncodedTile::DictEncodedTile(BackendType backend_type, TileGroupHeader *tile_header,
     const catalog::Schema &tuple_schema, TileGroup *tile_group,
     int tuple_count) : Tile(backend_type, tile_header, tuple_schema, tile_group, tuple_count),
     original_schema(tuple_schema),
     varlen_val_ptrs(nullptr) {
	std::vector<catalog::Column> columns;
	size_t offset = 0;

	// find out which column to encode
	for (oid_t column_idx = 0; column_idx < column_count; column_idx++) {
		auto column_type = schema.GetType(column_idx);
		original_schema_offsets.emplace(offset, column_idx);
		if (column_type == type::TypeId::VARCHAR ||
				column_type == type::TypeId::VARBINARY) {
			catalog::Column encoded_column(type::TypeId::INTEGER,
				type::Type::GetTypeSize(type::TypeId::INTEGER),
				schema.GetColumn(column_idx).GetName(), true);
			columns.push_back(encoded_column);
			dict_encoded_columns.emplace(column_idx, column_idx);
		} else {
			columns.push_back(schema.GetColumn(column_idx));
		}
		offset += schema.GetLength(column_idx);
	}

	// if nothing needs to be encoded
	if (dict_encoded_columns.empty()) return;

	delete[] data;
	schema = catalog::Schema(columns);
	tuple_length = schema.GetLength();
	tile_size = num_tuple_slots * tuple_length;
	data = new char[tile_size];
}

// delete data is enough
DictEncodedTile::~DictEncodedTile(){
	auto * str_ptrs = reinterpret_cast<const char**>(varlen_val_ptrs);
	for (size_t element_idx = 0; element_idx < dict.size(); element_idx++) {
		delete[] str_ptrs[element_idx];
	}
	delete[] varlen_val_ptrs;
}

type::Value DictEncodedTile::GetValue(const oid_t tuple_offset, const oid_t column_id) {
	if (dict_encoded_columns.count(column_id) > 0) {
		// encoded
		LOG_DEBUG("DictEncodedTile::GetValue called for tuple_offset: %d, column_id: %d",
			tuple_offset, column_id);
		auto idx_val = Tile::GetValue(tuple_offset, column_id);
		auto idx = idx_val.GetAs<uint32_t>();
		return type::Value::DeserializeFrom(varlen_val_ptrs + idx * sizeof(const char *),
		                                    original_schema.GetType(column_id), false);
	} else {
		return Tile::GetValue(tuple_offset, column_id);
	}
}

// assume offset is based on original schema, so get schema give original schema
type::Value DictEncodedTile::GetValueFast(const oid_t tuple_offset, const size_t column_offset,
                           const type::TypeId column_type,
                           const bool is_inlined){
  auto column_id = original_schema_offsets[column_offset];
	auto curr_val = Tile::GetValueFast(tuple_offset, schema.GetOffset(column_id),
	  column_type, is_inlined);
	if (dict_encoded_columns.count(column_id) > 0) {
		LOG_DEBUG("DictEncodedTile::GetValueFast called for tuple_offset: %d, column_id: %d",
			tuple_offset, column_id);
		auto idx_val = Tile::GetValue(tuple_offset, column_id);
		auto idx = idx_val.GetAs<uint32_t>();
		return type::Value::DeserializeFrom(varlen_val_ptrs + idx * sizeof(const char *),
		                                    original_schema.GetType(column_id), false);
	} else {
		return curr_val;
	}
}

void DictEncodedTile::DictEncode(Tile *tile) {
	PELOTON_ASSERT(tile != nullptr);
	if (is_dict_encoded) return;
	SetAttributes(tile);
	std::vector<type::Value> element_array;
	LOG_TRACE("dictionary encode, database_id: %d, table_id: %d, tile_group_id: %d"
				", tile_id: %d", database_id, table_id, tile_group_id, tile_id);

	// use a 2d vector to save the data
	std::vector<std::vector<type::Value>> new_data_vector(column_count);

	for (oid_t column_idx = 0; column_idx < column_count; column_idx++) {
		// if it is inlined, no need to compress!
		if (dict_encoded_columns.count(column_idx) > 0) {
			LOG_TRACE("encoding column %s", schema.GetColumn(column_idx).column_name.c_str());
			for (oid_t tuple_offset = 0; tuple_offset < num_tuple_slots; tuple_offset++) {
				type::Value curr_old_val = tile->GetValue(tuple_offset, column_idx);
				type::Value curr_val;
				if (curr_old_val.GetTypeId() == type::TypeId::VARBINARY) {
					curr_val = type::ValueFactory::GetVarbinaryValue((const unsigned char*) curr_old_val.GetData(),
					    curr_old_val.GetLength(), false);
				} else {
					curr_val = type::ValueFactory::GetVarcharValue(curr_old_val.GetData(), 
						curr_old_val.GetLength(), false);
				}
				// Since we use INTEGER as index, so the idx should take 4 bytes.
				char idx_data[4];
				if (dict.count(curr_val) == 0) {
					LOG_DEBUG("Encoding!!!!!");
					element_array.push_back(curr_val);
					*reinterpret_cast<uint32_t*>(idx_data) = element_array.size() - 1;
					dict.emplace(curr_val, *reinterpret_cast<uint32_t*>(idx_data));
				} else {
					*reinterpret_cast<uint32_t*>(idx_data) = dict[curr_val];
				}
				// many constructor of Value is private, so use
				// DeserializeFrom to construct the idx Value
				type::Value idx_val(type::Value::DeserializeFrom(idx_data, type::TypeId::INTEGER, true));
				new_data_vector[column_idx].push_back(idx_val);
			}
		} else {
			for (oid_t tuple_offset = 0; tuple_offset < num_tuple_slots; tuple_offset++) {
				new_data_vector[column_idx].push_back(tile->GetValue(tuple_offset, column_idx));
			}
		}
	}

	// now we have all data in new_data_vector
	// put new data into storage space
	for (oid_t column_idx = 0; column_idx < column_count; column_idx++) {
		for (oid_t tuple_offset = 0; tuple_offset < num_tuple_slots; tuple_offset++) {
			SetValueFast(new_data_vector[column_idx][tuple_offset], tuple_offset, schema.GetOffset(column_idx), 
				schema.IsInlined(column_idx), schema.GetLength(column_idx));
		}
	}

	// init varlen_val_ptrs
	varlen_val_ptrs = new char[dict.size() * sizeof(const char*)];
	for (size_t element_idx = 0; element_idx < dict.size(); element_idx++) {
		LOG_TRACE("element array [%zu] serialize to varlen_val_ptrs", element_idx);
		element_array[element_idx].SerializeTo(varlen_val_ptrs + element_idx * sizeof(const char *), 
			true, nullptr);
	}
	is_dict_encoded = true;
}

Tile* DictEncodedTile::DictDecode() {
	if (!is_dict_encoded) return nullptr;
	LOG_DEBUG("dictionary decode, database_id: %d, table_id: %d, tile_group_id: %d"
				", tile_id: %d", database_id, table_id, tile_group_id, tile_id);
  	// new data mean decoded data
  	std::vector<std::vector<type::Value>> new_data_vector(column_count);
  	for (oid_t column_idx = 0; column_idx < column_count; column_idx++) {
    	if (dict_encoded_columns.count(column_idx) > 0) {
      		LOG_DEBUG("decoding column %s", schema.GetColumn(column_idx).column_name.c_str());
      		for (oid_t tuple_offset = 0; tuple_offset < num_tuple_slots; tuple_offset++) {
        		type::Value curr_val = Tile::GetValue(tuple_offset, column_idx);
        		auto idx = curr_val.GetAs<uint32_t>();
        		type::Value actual_val = type::Value::DeserializeFrom(varlen_val_ptrs + idx * sizeof(const char *),
		                                    original_schema.GetType(column_idx), false);
       			new_data_vector[column_idx].push_back(actual_val);
      		}
    	} else {
      		for (oid_t tuple_offset = 0; tuple_offset < num_tuple_slots; tuple_offset++) {
        		new_data_vector[column_idx].push_back(GetValue(tuple_offset, column_idx));
      		}
    	}
	}

	// put the decoded data to newly-created tile
	auto *new_tile = new Tile(backend_type, tile_group_header, original_schema, tile_group, num_tuple_slots);
  	for (oid_t column_idx = 0; column_idx < column_count; column_idx++) {
		for (oid_t tuple_offset = 0; tuple_offset < num_tuple_slots; tuple_offset++) {
			new_tile->SetValueFast(new_data_vector[column_idx][tuple_offset], tuple_offset, 
				original_schema.GetOffset(column_idx), original_schema.IsInlined(column_idx),
				type::Type::GetTypeSize(original_schema.GetType(column_idx)));
		}
	}
	new_tile->SetAttributes(this);
	LOG_DEBUG("END of decoding");
	is_dict_encoded = false;
	return new_tile;
}

} // namespace storage
} // namespace peloton