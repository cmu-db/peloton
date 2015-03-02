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
Tile* TileFactory::GetTile(catalog::Schema* schema, int tuple_count,
		const std::vector<std::string>& column_names,
		const bool owns_tuple_schema) {

	Backend* backend = new storage::VolatileBackend();

	return TileFactory::GetTile(InvalidOid, InvalidOid,
			InvalidOid, InvalidOid, backend, schema, tuple_count,
			column_names, owns_tuple_schema);
}


Tile* TileFactory::GetTile(Oid database_id, Oid table_id,
		Oid tile_group_id, Oid tile_id,
		Backend* backend, catalog::Schema* schema, int tuple_count,
		const std::vector<std::string>& column_names,
		const bool owns_tuple_schema) {

	Tile *tile = new Tile(backend, schema, tuple_count, column_names,
			owns_tuple_schema);

	TileFactory::InitCommon(tile, database_id, table_id, tile_group_id,
			schema, column_names, owns_tuple_schema);

	// initialize tile stats

	return tile;
}

void TileFactory::InitCommon(Tile *tile, Oid database_id, Oid table_id,
		Oid tile_group_id, catalog::Schema* schema, const std::vector<std::string>& column_names,
		const bool owns_tuple_schema) {

	tile->database_id = database_id;
	tile->tile_group_id = tile_group_id;
	tile->table_id = table_id;

	tile->schema = schema;
	tile->column_names = column_names;
	tile->own_schema = owns_tuple_schema;

}


} // End storage namespace
} // End nstore namespace
