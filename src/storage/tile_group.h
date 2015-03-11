/*-------------------------------------------------------------------------
 *
 * tile_group.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile_group.h
 *
 *-------------------------------------------------------------------------
 */

#include "storage/tile.h"
#include "storage/tile_group_header.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group
//===--------------------------------------------------------------------===//

class TileGroupIterator;

/**
 * Represents a group of tiles logically horizontally contiguous.
 *
 * < <Tile 1> <Tile 2> .. <Tile n> >
 *
 * Look at TileGroupHeader for MVCC implementation.
 *
 * TileGroups are only instantiated via TileGroupFactory.
 */
class TileGroup {
	friend class Tile;
	friend class TileGroupFactory;

	TileGroup() = delete;
	TileGroup(TileGroup const&) = delete;

public:

	// Tile group constructor
	TileGroup(TileGroupHeader* tile_header,
			Backend* backend,
			std::vector<catalog::Schema *> schemas,
			int tuple_count,
			const std::vector<std::vector<std::string> >& column_names,
			bool own_schema);

	~TileGroup() {
		// clean up tile group header
		delete tile_group_header;

		// clean up tiles
		for(auto tile : tiles)
			delete tile;
	}

	//===--------------------------------------------------------------------===//
	// Utilities
	//===--------------------------------------------------------------------===//

	bool InsertTuple(Tuple &source);

	// Get a string representation of this tile group
	friend std::ostream& operator<<(std::ostream& os, const TileGroup& tile_group);

protected:

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	// storage backend
	Backend *backend;

	// set of tiles
	std::vector<Tile*> tiles;

	TileGroupHeader* tile_group_header;

	// number of tuple slots allocated
	id_t num_tuple_slots;

	// number of tiles
	id_t tile_count;

	// mapping to tile schemas
	std::vector<catalog::Schema *> tile_schemas;

	// Catalog information
	id_t tile_group_id;
	id_t table_id;
	id_t database_id;

};

//===--------------------------------------------------------------------===//
// Tile Group factory
//===--------------------------------------------------------------------===//

class TileGroupFactory {
public:
	TileGroupFactory();
	virtual ~TileGroupFactory();

	static TileGroup *GetTileGroup(const std::vector<catalog::Schema*>& schemas,
			Backend* backend,
			int tuple_count,
			const std::vector<std::vector<std::string> >& column_names,
			const bool owns_tuple_schema){

		// create backend if needed
		if(backend == NULL)
			backend = new storage::VMBackend();

		return TileGroupFactory::GetTileGroup(INVALID_ID, INVALID_ID, INVALID_ID,
				NULL, schemas, backend, tuple_count, column_names, owns_tuple_schema);
	}

	static TileGroup *GetTileGroup(id_t database_id,
			id_t table_id,
			id_t tile_group_id,
			TileGroupHeader* tile_header,
			const std::vector<catalog::Schema*>& schemas,
			Backend* backend,
			int tuple_count,
			const std::vector<std::vector<std::string> >& column_names,
			const bool owns_tuple_schema) {

		// create tile header if needed
		if(tile_header == NULL)
			tile_header = new TileGroupHeader(backend, tuple_count);

		TileGroup *tile_group = new TileGroup(tile_header, backend, schemas, tuple_count,
				column_names, owns_tuple_schema);

		TileGroupFactory::InitCommon(tile_group, database_id, table_id, tile_group_id);

		return tile_group;
	}

private:

	static void InitCommon(TileGroup *tile_group,
			id_t database_id,
			id_t table_id,
			id_t tile_group_id) {

		tile_group->database_id = database_id;
		tile_group->tile_group_id = tile_group_id;
		tile_group->table_id = table_id;

	}

};
} // End storage namespace
} // End nstore namespace

