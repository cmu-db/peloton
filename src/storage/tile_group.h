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

#pragma once

#include "storage/tile.h"
#include "storage/tile_factory.h"
#include "storage/tile_group_header.h"
#include "catalog/catalog.h"

#include <cassert>

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
	    catalog::Catalog *catalog,
			Backend* backend,
			std::vector<catalog::Schema *> schemas,
			int tuple_count,
			const std::vector<std::vector<std::string> >& column_names,
			bool own_schema);

	~TileGroup() {
		// clean up tiles
		for(auto tile : tiles)
			delete tile;

    // clean up tile group header
    delete tile_group_header;

		// own backend ?
		if(own_backend)
		  delete backend;
	}

	//===--------------------------------------------------------------------===//
	// Operations
	//===--------------------------------------------------------------------===//

	// insert tuple at next available slot in tile if a slot exists
	id_t InsertTuple(txn_id_t transaction_id, const Tuple *tuple);

	// returns tuple at given slot if it exists and is visible to transaction at this time stamp
	Tuple* SelectTuple(txn_id_t transaction_id, id_t tile_id, id_t tuple_slot_id, cid_t at_cid);

	// returns tuples present in tile and are visible to transaction at this time stamp
	Tile* ScanTuples(txn_id_t transaction_id, id_t tile_id, cid_t at_cid);

	// delete tuple at given slot if it is not already locked
	bool DeleteTuple(txn_id_t transaction_id, id_t tuple_slot_id);

	//===--------------------------------------------------------------------===//
	// Utilities
	//===--------------------------------------------------------------------===//

	// Get a string representation of this tile group
	friend std::ostream& operator<<(std::ostream& os, const TileGroup& tile_group);

	id_t GetActiveTupleCount() const {
		return tile_group_header->GetActiveTupleCount();
	}

	TileGroupHeader *GetHeader() const{
		return tile_group_header;
	}

  oid_t GetTileId(const id_t tile_id) const {
    return tiles[tile_id]->GetTileId();
  }

	Pool *GetTilePool(const id_t tile_id) const {
	  return tiles[tile_id]->GetPool();
	}

protected:

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	// set of tiles
	std::vector<Tile*> tiles;

	TileGroupHeader* tile_group_header;

	// number of tuple slots allocated
	id_t num_tuple_slots;

	// number of tiles
	id_t tile_count;

	// mapping to tile schemas
	std::vector<catalog::Schema *> tile_schemas;

	// backend
	Backend* backend;

	// own backend ?
	bool own_backend;

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
			int tuple_count,
			const std::vector<std::vector<std::string> >& column_names,
			const bool owns_tuple_schema,
			catalog::Catalog *catalog,
			Backend *backend = nullptr){

	  bool own_backend = false;
		// create backend if needed
		if(backend == nullptr) {
			backend = new storage::VMBackend();
			own_backend = true;
		}

		TileGroup *group = TileGroupFactory::GetTileGroup(INVALID_OID, INVALID_OID, INVALID_OID,
				nullptr, schemas, catalog, backend, tuple_count, column_names, owns_tuple_schema);

    group->backend = backend;

		if(own_backend) {
		  group->own_backend = true;
		}

		return group;
	}

	static TileGroup *GetTileGroup(oid_t database_id,
			oid_t table_id,
			oid_t tile_group_id,
			TileGroupHeader* tile_header,
			const std::vector<catalog::Schema*>& schemas,
			catalog::Catalog *catalog,
			Backend* backend,
			int tuple_count,
			const std::vector<std::vector<std::string> >& column_names,
			const bool owns_tuple_schema) {

		// create tile header if needed
		if(tile_header == nullptr)
			tile_header = new TileGroupHeader(backend, tuple_count);

		TileGroup *tile_group = new TileGroup(tile_header, catalog, backend, schemas, tuple_count,
				column_names, owns_tuple_schema);

		TileGroupFactory::InitCommon(tile_group, database_id, table_id, tile_group_id);

		return tile_group;
	}

private:

	static void InitCommon(TileGroup *tile_group,
			oid_t database_id,
			oid_t table_id,
			oid_t tile_group_id) {

		tile_group->database_id = database_id;
		tile_group->tile_group_id = tile_group_id;
		tile_group->table_id = table_id;

	}

};
} // End storage namespace
} // End nstore namespace

