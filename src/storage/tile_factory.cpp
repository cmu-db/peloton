/*-------------------------------------------------------------------------
 *
 * tile_factory.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile_factory.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/tile_factory.h"


namespace nstore {
namespace storage {

/// Uses volatile backend and default catalog information
PhysicalTile* TileFactory::GetPhysicalTile(catalog::Schema* schema, int tuple_count,
		const std::vector<std::string>& column_names,
		const bool owns_tuple_schema, int tile_size) {

	Backend* backend = new storage::VolatileBackend();

	return TileFactory::GetPhysicalTile(0, 0, 0, backend, schema, tuple_count, column_names,
			owns_tuple_schema, tile_size);
}


PhysicalTile* TileFactory::GetPhysicalTile(Oid database_id, Oid table_id,
		Oid tile_group_id, Backend* backend, catalog::Schema* schema, int tuple_count,
		const std::vector<std::string>& column_names,
		const bool owns_tuple_schema, int tile_size) {

	PhysicalTile *tile = new PhysicalTile(backend, schema, tuple_count, column_names,
			owns_tuple_schema);

	TileFactory::InitCommon(tile, database_id, table_id, tile_group_id,
			schema, column_names, owns_tuple_schema, tile_size);

	// initialize tile stats

	return tile;
}

void TileFactory::InitCommon(PhysicalTile *tile, Oid database_id, Oid table_id,
		Oid tile_group_id, catalog::Schema* schema, const std::vector<std::string>& column_names,
		const bool owns_tuple_schema, int tile_size) {

	tile->database_id = database_id;
	tile->tile_group_id = tile_group_id;
	tile->table_id = table_id;

	tile->schema = schema;
	tile->column_names = column_names;
	tile->own_schema = owns_tuple_schema;

	tile->tile_size = tile_size;
}


} // End storage namespace
} // End nstore namespace
