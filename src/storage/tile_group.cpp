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

#include <numeric>

#include "common/logger.h"
#include "common/synch.h"
#include "common/types.h"

namespace nstore {
namespace storage {

TileGroup::TileGroup(TileGroupHeader* tile_group_header,
                     Backend* backend,
                     const std::vector<catalog::Schema>& schemas,
                     int tuple_count)
: database_id(INVALID_ID),
  table_id(INVALID_ID),
  tile_group_id(INVALID_ID),
  backend(backend),
  tile_schemas(schemas),
  tile_group_header(tile_group_header),
  num_tuple_slots(tuple_count) {

  tile_count = tile_schemas.size();

  for(id_t tile_itr = 0 ; tile_itr < tile_count ; tile_itr++){

    oid_t tile_id = catalog::Manager::GetInstance().GetNextOid();

    Tile *tile = storage::TileFactory::GetTile(
        database_id, table_id, tile_group_id, tile_id,
        tile_group_header,
        backend,
        tile_schemas[tile_itr],
        this,
        tuple_count);

    // add tile metadata in locator
    catalog::Manager::GetInstance().SetLocation(tile_id, tile);

    tiles.push_back(tile_id);
  }

}

//===--------------------------------------------------------------------===//
// Operations
//===--------------------------------------------------------------------===//

/**
 * Grab next slot (thread-safe) and fill in the tuple
 *
 * Returns slot where inserted (INVALID_ID if not inserted)
 */
id_t TileGroup::InsertTuple(txn_id_t transaction_id, const Tuple *tuple) {

  id_t tuple_slot_id = tile_group_header->GetNextEmptyTupleSlot();

  LOG_TRACE("Tile Group Id :: %lu status :: %lu out of %lu slots \n", tile_group_id, tuple_slot_id, num_tuple_slots);

  // No more slots
  if(tuple_slot_id == INVALID_ID)
    return INVALID_ID;

  id_t tile_column_count;
  id_t column_itr = 0;

  for(id_t tile_itr = 0 ; tile_itr < tile_count ; tile_itr++){
    const catalog::Schema& schema = tile_schemas[tile_itr];
    tile_column_count = schema.GetColumnCount();

    storage::Tile *tile = GetTile(tile_itr);
    assert(tile);
    char* tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    assert(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    storage::Tuple tile_tuple(&schema, tile_tuple_location);

    for(id_t tile_column_itr = 0 ; tile_column_itr < tile_column_count ; tile_column_itr++){
      tile_tuple.SetValue(tile_column_itr, tuple->GetValue(column_itr));
      column_itr++;
    }

    // Set MVCC info
    tile_group_header->SetTransactionId(tuple_slot_id, transaction_id);
    tile_group_header->SetBeginCommitId(tuple_slot_id, MAX_CID);
    tile_group_header->SetEndCommitId(tuple_slot_id, MAX_CID);
  }

  return tuple_slot_id;
}

void TileGroup::ReclaimTuple(id_t tuple_slot_id) {

  // add it to free slots
  tile_group_header->ReclaimTupleSlot(tuple_slot_id);

}

/**
 * Return tuple at given tile slot if it exists and is visible.
 *
 * USED FOR POINT LOOKUPS
 */
Tuple *TileGroup::SelectTuple(txn_id_t transaction_id, id_t tile_offset, id_t tuple_slot_id, cid_t at_cid) {
  assert(tile_offset < tile_count);
  assert(tuple_slot_id < num_tuple_slots);

  // is it within bounds ?
  if(tuple_slot_id >= GetActiveTupleCount())
    return nullptr;

  Tile *tile = GetTile(tile_offset);
  if(tile == nullptr)
    return nullptr;

  Tuple *tuple = nullptr;
  // is it visible to transaction ?
  if(tile_group_header->IsVisible(tuple_slot_id, transaction_id, at_cid)){
    tuple = tile->GetTuple(tuple_slot_id);
    return tuple;
  }

  return nullptr;
}

/**
 * Return tuples in tile that exist and are visible.
 *
 * USED FOR SEQUENTIAL SCANS
 */
Tile *TileGroup::ScanTuples(txn_id_t transaction_id, id_t tile_offset, cid_t at_cid) {
  assert(tile_offset < tile_count);

  id_t active_tuple_count = GetActiveTupleCount();

  // does it have tuples ?
  if(active_tuple_count == 0)
    return nullptr;

  Tuple *tuple = nullptr;

  Tile *tile = GetTile(tile_offset);
  if(tile == nullptr)
    return nullptr;

  std::vector<Tuple *> tuples;

  // else go over all tuples
  for(id_t tile_itr = 0 ; tile_itr < active_tuple_count ; tile_itr++){

    // is tuple at this slot visible to transaction ?
    if(tile_group_header->IsVisible(tile_itr, transaction_id, at_cid)){
      tuple = tile->GetTuple(tile_itr);
      tuples.push_back(tuple);
    }
  }

  // create a new tile and insert these tuples
  id_t tuple_count = tuples.size();

  if(tuple_count > 0) {
    TileGroupHeader* header = new TileGroupHeader(backend, tuple_count);

    storage::Tile *tile = storage::TileFactory::GetTile(
        INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
        header, backend, tile_schemas[tile_offset], this, tuple_count);

    id_t tuple_slot_id = 0;

    for(auto tuple : tuples) {
      tile->InsertTuple(tuple_slot_id, tuple);
      tuple_slot_id++;
      delete tuple;
    }

    return tile;
  }

  return nullptr;
}

// delete tuple at given slot if it is not already locked
bool TileGroup::DeleteTuple(txn_id_t transaction_id, id_t tuple_slot_id) {

  // compare and exchange the end commit id
  if (atomic_cas<txn_id_t>(tile_group_header->GetEndCommitIdLocation(tuple_slot_id),
                           MAX_CID, transaction_id)) {
    return true;
  }

  return false;
}

// Get tile id and column id w.r.t that tile corresponding to the specified
// tile group column id.
void TileGroup::LocateTileAndColumn(
    id_t column_id,
    id_t &tile_column_id,
    id_t &tile_id) {
  tile_column_id = column_id;
  tile_id = 0;
  assert(tile_schemas.size() > 0);
  while (tile_column_id >= tile_schemas[tile_id].GetColumnCount()) {
    tile_column_id -= tile_schemas[tile_id].GetColumnCount();
    tile_id++;
    assert(tile_id < tile_schemas.size());
  }
  assert(tile_id < tiles.size());
}

id_t TileGroup::GetTileIdFromColumnId(id_t column_id) {
  id_t tile_column_id, tile_id;
  LocateTileAndColumn(column_id, tile_column_id, tile_id);
  return tile_id;
}

id_t TileGroup::GetTileColumnId(id_t column_id) {
  id_t tile_column_id, tile_id;
  LocateTileAndColumn(column_id, tile_column_id, tile_id);
  return tile_column_id;
}

Value TileGroup::GetValue(id_t tuple_id, id_t column_id) {
  assert(tuple_id < GetActiveTupleCount());
  id_t tile_column_id, tile_offset;
  LocateTileAndColumn(column_id, tile_column_id, tile_offset);
  return GetTile(tile_offset)->GetValue(tuple_id, tile_column_id);
}

Tile *TileGroup::GetTile(const id_t tile_offset) const {
  assert(tile_offset < tile_count);
  auto& manager = catalog::Manager::GetInstance();
  Tile *tile = static_cast<Tile *>(manager.GetLocation(tiles[tile_offset]));
  return tile;
}


//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

// Get a string representation of this tile group
std::ostream& operator<<(std::ostream& os, const TileGroup& tile_group) {

  os << "=============================================================\n";

  os << "TILE GROUP :\n";
  os << "\tCatalog ::"
      << " DB: "<< tile_group.database_id << " Table: " << tile_group.table_id
      << " Tile Group:  " << tile_group.tile_group_id
      << "\n";

  os << "\tActive Tuples:  " << tile_group.tile_group_header->GetActiveTupleCount()
													                                                << " out of " << tile_group.num_tuple_slots  <<" slots\n";

  for(id_t tile_itr = 0 ; tile_itr < tile_group.tile_count ; tile_itr++){
    Tile *tile = tile_group.GetTile(tile_itr);
    if(tile != nullptr)
      os << (*tile);
  }

  os << "=============================================================\n";

  return os;
}

} // End storage namespace
} // End nstore namespace
