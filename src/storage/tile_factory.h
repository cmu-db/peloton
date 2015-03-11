/*-------------------------------------------------------------------------
 *
 * tile_factory.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile_factory.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/schema.h"
#include "storage/tile.h"
#include "storage/backend.h"
#include "storage/volatile_backend.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile factory
//===--------------------------------------------------------------------===//

class TileFactory {
public:
	TileFactory();
	virtual ~TileFactory();

	static Tile *GetTile(catalog::Schema* schema,
			int tuple_count, const std::vector<std::string>& column_names,
			const bool owns_tuple_schema){

		Backend* backend = new storage::VolatileBackend();

		return TileFactory::GetTile(INVALID_ID, INVALID_ID,
				INVALID_ID, INVALID_ID, backend, schema, tuple_count,
				column_names, owns_tuple_schema);
	}

	static Tile *GetTile(id_t database_id, id_t table_id, id_t tile_group_id, id_t tile_id,
			Backend* backend, catalog::Schema* schema, int tuple_count,
			const std::vector<std::string>& column_names,
			const bool owns_tuple_schema){

		Tile *tile = new Tile(backend, schema, tuple_count, column_names,
				owns_tuple_schema);

		TileFactory::InitCommon(tile, database_id, table_id, tile_group_id,
				schema, column_names, owns_tuple_schema);

		// initialize tile stats

		return tile;
	}

private:

	static void InitCommon(Tile *tile, id_t database_id, id_t table_id,
			id_t tile_group_id, catalog::Schema* schema,
			const std::vector<std::string>& column_names,
			const bool owns_tuple_schema) {

		tile->database_id = database_id;
		tile->tile_group_id = tile_group_id;
		tile->table_id = table_id;

		tile->schema = schema;
		tile->column_names = column_names;
		tile->own_schema = owns_tuple_schema;

	}

};

} // End storage namespace
} // End nstore namespace



