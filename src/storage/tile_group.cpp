/*-------------------------------------------------------------------------
 *
 * tile_group.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile_group.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/tile_group.h"

namespace nstore {
namespace storage {

TileGroup::TileGroup(TileGroupHeader* _tile_group_header,
		Backend* _backend,
		std::vector<catalog::Schema *> schemas,
		int tuple_count,
		const std::vector<std::vector<std::string> >& column_names,
		bool own_schema)
: tile_group_header(_tile_group_header),
  num_tuple_slots(tuple_count),
  tile_schemas(schemas),
  tile_group_id(INVALID_ID),
  table_id(INVALID_ID),
  database_id(INVALID_ID) {

	tile_count = tile_schemas.size();

	for(id_t tile_itr = 0 ; tile_itr < tile_count ; tile_itr++){
		Tile * tile = storage::TileFactory::GetTile(tile_schemas[tile_itr],
				tuple_count, column_names[tile_itr], own_schema,
				tile_group_header, _backend);

		tiles.push_back(tile);
	}

}


bool TileGroup::InsertTuple(Tuple &source) {

	id_t tuple_slot_id = tile_group_header->GetNextEmptyTupleSlot();

	//std::cout << "Empty tuple slot :: " << tuple_slot_id << "\n";

	// No more slots
	if(tuple_slot_id == INVALID_ID)
		return false;

	id_t tile_column_count;
	id_t column_itr = 0;

	for(id_t tile_itr = 0 ; tile_itr < tile_count ; tile_itr++){
		catalog::Schema *schema = tile_schemas[tile_itr];
		tile_column_count = schema->GetColumnCount();

		storage::Tuple *tuple = new storage::Tuple(schema, true);

		for(id_t tile_column_itr = 0 ; tile_column_itr < tile_column_count ; tile_column_itr++){
			tuple->SetValue(tile_column_itr, source.GetValue(column_itr));
			column_itr++;
		}

		tiles[tile_itr]->InsertTuple(tuple_slot_id, tuple);
	}

	return true;
}

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

// Get a string representation of this tile group
std::ostream& operator<<(std::ostream& os, const TileGroup& tile_group) {


	os << "====================================================================================================\n";

	os << "TILE GROUP :\n";
	os << "\tCatalog ::"
			<< " DB: "<< tile_group.database_id << " Table: " << tile_group.table_id
			<< " Tile Group:  " << tile_group.tile_group_id
			<< "\n";

	os << "\tActive Tuples:  " << tile_group.tile_group_header->GetActiveTupleCount()
			<< " out of " << tile_group.num_tuple_slots  <<" slots\n";

	for(id_t tile_itr = 0 ; tile_itr < tile_group.tile_count ; tile_itr++){
		os << (*tile_group.tiles[tile_itr]);
	}

	os << "====================================================================================================\n";

	return os;
}

} // End storage namespace
} // End nstore namespace
