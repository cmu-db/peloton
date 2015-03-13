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

#include "storage/static_tile.h"
#include "storage/tile.h"
#include "storage/backend.h"
#include "storage/vm_backend.h"

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
			int tuple_count,
			const std::vector<std::string>& column_names,
			const bool owns_tuple_schema,
			TileGroupHeader* tile_header = nullptr,
			Backend* backend = nullptr){

		// create backend if needed
		if(backend == nullptr)
			backend = new storage::VMBackend();

		return TileFactory::GetTile(INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
				tile_header, schema, backend, tuple_count,
				column_names, owns_tuple_schema);
	}

	static Tile *GetTile(oid_t database_id,
			oid_t table_id,
			oid_t tile_group_id,
			oid_t tile_id,
			TileGroupHeader* tile_header,
			catalog::Schema* schema,
			Backend* backend,
			int tuple_count,
			const std::vector<std::string>& column_names,
			const bool owns_tuple_schema) {

		// create tile header if this is not a static tile and is needed
		if(tile_header == nullptr)
			tile_header = new TileGroupHeader(backend, tuple_count);

		Tile *tile = new Tile(tile_header, backend, schema, tuple_count, column_names,
				owns_tuple_schema);

		TileFactory::InitCommon(tile, database_id, table_id, tile_group_id, tile_id,
				schema, column_names, owns_tuple_schema);

		return tile;
	}

	// STATIC TILE

	static Tile *GetStaticTile(catalog::Schema* schema,
			int tuple_count,
			const std::vector<std::string>& column_names,
			const bool owns_tuple_schema,
			Backend* backend = nullptr){

		// create backend if needed
		if(backend == nullptr)
			backend = new storage::VMBackend();

		return TileFactory::GetStaticTile(INVALID_OID, INVALID_OID,
				INVALID_OID, INVALID_OID,
				schema, backend, tuple_count,
				column_names, owns_tuple_schema);
	}

	static Tile *GetStaticTile(oid_t database_id,
			oid_t table_id,
			oid_t tile_group_id,
			oid_t tile_id,
			catalog::Schema* schema,
			Backend* backend,
			int tuple_count,
			const std::vector<std::string>& column_names,
			const bool owns_tuple_schema) {

		StaticTile *tile = new StaticTile(backend, schema, tuple_count, column_names,
				owns_tuple_schema);

		TileFactory::InitCommon(tile, database_id, table_id, tile_group_id, tile_id,
				schema, column_names, owns_tuple_schema);

		return tile;
	}

private:

	static void InitCommon(Tile *tile,
			oid_t database_id,
			oid_t table_id,
			oid_t tile_id,
			oid_t tile_group_id,
			catalog::Schema* schema,
			const std::vector<std::string>& column_names,
			const bool owns_tuple_schema) {

		tile->database_id = database_id;
		tile->tile_group_id = tile_group_id;
		tile->table_id = table_id;
		tile->tile_id = tile_id;

		tile->schema = schema;
		tile->column_names = column_names;
		tile->own_schema = owns_tuple_schema;

	}

};


} // End storage namespace
} // End nstore namespace

