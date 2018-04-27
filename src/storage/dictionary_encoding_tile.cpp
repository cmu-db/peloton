
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
#include "storage/dictionary_encoding_tile.h"

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
	for (oid_t i = 0; i < column_count; i++) {
		auto column_type = schema.GetType(i);
		original_schema_offsets.emplace(offset, i);
		if (column_type == type::TypeId::VARCHAR ||
				column_type == type::TypeId::VARBINARY) {
			catalog::Column encoded_column(type::TypeId::INTEGER,
				type::Type::GetTypeSize(type::TypeId::INTEGER),
				schema.GetColumn(i).GetName(), true);
			columns.push_back(encoded_column);
			dict_encoded_columns.emplace(i, i);
//			dict_encoded_columns.insert(i);
		} else {
			columns.push_back(schema.GetColumn(i));
		}
		offset += schema.GetLength(i);
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
	delete[] varlen_val_ptrs;
}

type::Value DictEncodedTile::GetValue(const oid_t tuple_offset, const oid_t column_id) {
	if (dict_encoded_columns.count(column_id) > 0) {
		// encoded
		LOG_INFO("DictEncodedTile::GetValue called for tuple_offset: %d, column_id: %d",
			tuple_offset, column_id);
		auto idx_val = Tile::GetValue(tuple_offset, column_id);
		auto idx = idx_val.GetAs<uint8_t>();
    return type::Value(element_array[idx]);
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
		LOG_INFO("DictEncodedTile::GetValueFast called for tuple_offset: %d, column_id: %d",
			tuple_offset, column_id);
		auto idx_val = Tile::GetValue(tuple_offset, column_id);
		auto idx = idx_val.GetAs<uint8_t>();
    return type::Value(element_array[idx]);
	} else {
		return curr_val;
	}
}

//void DictEncodedTile::SetValue(const type::Value &value, const oid_t tuple_offset,
//                const oid_t column_id){
//
//}

void DictEncodedTile::DictEncode(Tile *tile) {
	if (is_dict_encoded) return;
	SetAttributes(tile);

	LOG_INFO("dictionary encode, database_id: %d, table_id: %d, tile_group_id: %d"
				", tile_id: %d", database_id, table_id, tile_group_id, tile_id);

	// use a 2d vector to save the data
	std::vector<std::vector<type::Value>> new_data_vector(column_count);

	for (oid_t i = 0; i < column_count; i++) {
		// if it is inlined, no need to compress!
		if (dict_encoded_columns.count(i) > 0) {
			LOG_INFO("encoding column %s", schema.GetColumn(i).column_name.c_str());
			// to for tuple offset
			for (oid_t to = 0; to < num_tuple_slots; to++) {
				type::Value curr_old_val = tile->GetValue(to, i);
				type::Value curr_val;
				if (curr_old_val.GetTypeId() == type::TypeId::VARBINARY) {
					curr_val = type::ValueFactory::GetVarbinaryValue((const unsigned char*) curr_old_val.GetData(),
					    curr_old_val.GetLength(), true);
				} else {
					curr_val = type::ValueFactory::GetVarcharValue(curr_old_val.GetData(), curr_old_val.GetLength(),
							true);
				}
//				LOG_INFO("%s", curr_val.GetInfo().c_str());
				// assume the idx take 1 byte
				char idx_data[1];
				if (dict.count(curr_val) == 0) {
					LOG_INFO("Encoding!!!!!");
					element_array.push_back(curr_val);
					idx_data[0] = element_array.size() - 1;
					dict.emplace(curr_val, idx_data[0]);
				} else {
//					LOG_INFO("Already encoded!!!");
					idx_data[0] = dict[curr_val];
				}
				// many constructor of Value is private, so use
				// DeserializeFrom to construct the idx Value
				type::Value idx_val(type::Value::DeserializeFrom(idx_data, type::TypeId::INTEGER, true));
				new_data_vector[i].push_back(idx_val);
			}
		} else {
			for (oid_t to = 0; to < num_tuple_slots; to++) {
				new_data_vector[i].push_back(tile->GetValue(to, i));
			}
		}
	}

	// now we have all data in new_data_vector
	// put new data into storage space

	for (oid_t i = 0; i < column_count; i++) {
		for (oid_t to = 0; to < num_tuple_slots; to++) {
			SetValueFast(new_data_vector[i][to], to, schema.GetOffset(i), schema.IsInlined(i),
					schema.GetLength(i));
		}
	}

	// init varlen_val_ptrs
//	varlen_val_ptrs = new const char*[element_array.size()];
	varlen_val_ptrs = new char[element_array.size() * sizeof(const char*)];
	for (size_t i = 0; i < element_array.size(); i++) {
//		varlen_val_ptrs[i] = element_array[i].GetData();
		const char *varlen_data = element_array[i].GetData();
//		char *storage = *reinterpret_cast<char **>(varlen_val_ptrs + i);
		PELOTON_MEMCPY(varlen_val_ptrs + i * sizeof(const char*), &varlen_data, sizeof(const char*));
	}
	is_dict_encoded = true;
}

Tile* DictEncodedTile::DictDecode() {
	if (!is_dict_encoded) return nullptr;
  LOG_INFO("dictionary decode, database_id: %d, table_id: %d, tile_group_id: %d"
					", tile_id: %d", database_id, table_id, tile_group_id, tile_id);
  // new data mean decoded data
  std::vector<std::vector<type::Value>> new_data_vector(column_count);
  for (oid_t i = 0; i < column_count; i++) {
    if (dict_encoded_columns.count(i) > 0) {
      LOG_INFO("decoding column %s", schema.GetColumn(i).column_name.c_str());
      for (oid_t to = 0; to < num_tuple_slots; to++) {
        type::Value curr_val = Tile::GetValue(to, i);
        auto idx = curr_val.GetAs<uint8_t>();
        type::Value actual_val = element_array[idx];
        new_data_vector[i].push_back(actual_val);
      }
    } else {
      for (oid_t to = 0; to < num_tuple_slots; to++) {
        new_data_vector[i].push_back(GetValue(to, i));
      }
    }
	}

  auto *new_tile = new Tile(backend_type, tile_group_header, *GetSchema(), tile_group, num_tuple_slots);
  for (oid_t i = 0; i < column_count; i++) {
		for (oid_t to = 0; to < num_tuple_slots; to++) {
			new_tile->SetValueFast(new_data_vector[i][to], to, original_schema.GetOffset(i), original_schema.IsInlined(i),
					type::Type::GetTypeSize(original_schema.GetType(i)));
		}
	}

//	new_tile->database_id = database_id;
//	new_tile->table_id = table_id;
//	new_tile->tile_group_id = tile_group_id;
//	new_tile->tile_id = tile_id;
//	new_tile->tile_group_header = tile_group_header;
//	new_tile->tile_group = tile_group;
//	new_tile->backend_type = backend_type;
	new_tile->SetAttributes(this);
	LOG_INFO("END of decoding");
	is_dict_encoded = false;
	return new_tile;
}

} // namespace storage
} // namespace peloton