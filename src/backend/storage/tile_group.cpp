//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_group.cpp
//
// Identification: src/backend/storage/tile_group.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/tile_group.h"

#include <numeric>

#include "backend/catalog/manager.h"
#include "backend/common/logger.h"
#include "backend/common/synch.h"
#include "backend/common/types.h"
#include "backend/storage/abstract_table.h"
#include "backend/storage/tile.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace storage {

TileGroup::TileGroup(TileGroupHeader *tile_group_header, AbstractTable *table,
                     const std::vector<catalog::Schema> &schemas,
                     const column_map_type &column_map, int tuple_count)
    : database_id(INVALID_OID),
      table_id(INVALID_OID),
      tile_group_id(INVALID_OID),
      tile_schemas(schemas),
      tile_group_header(tile_group_header),
      table(table),
      num_tuple_slots(tuple_count),
      column_map(column_map),
      ref_count(BASE_REF_COUNT) {
  tile_count = tile_schemas.size();

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    auto &manager = catalog::Manager::GetInstance();
    oid_t tile_id = manager.GetNextOid();

    Tile *tile = storage::TileFactory::GetTile(
        database_id, table_id, tile_group_id, tile_id, tile_group_header,
        tile_schemas[tile_itr], this, tuple_count);

    tiles.push_back(tile);
  }
}

TileGroup::~TileGroup() {
  // clean up tiles
  for (auto tile : tiles) {
    tile->DecrementRefCount();
  }

  // clean up tile group header
  delete tile_group_header;
}

void TileGroup::IncrementRefCount() {
  ++ref_count;
}

oid_t TileGroup::GetTileId(const oid_t tile_id) const {
  assert(tiles[tile_id]);
  return tiles[tile_id]->GetTileId();
}

peloton::VarlenPool *TileGroup::GetTilePool(const oid_t tile_id) const {
   Tile *tile = GetTile(tile_id);

   if (tile != nullptr) return tile->GetPool();

   return nullptr;
 }


void TileGroup::DecrementRefCount() {
  // DROP tile group when ref count reaches 0
  // this returns the value immediately preceding the assignment
  if (ref_count.fetch_sub(1) == BASE_REF_COUNT) {
    delete this;
  }
}

oid_t TileGroup::GetNextTupleSlot() const {
  return tile_group_header->GetNextTupleSlot();
}

oid_t TileGroup::GetActiveTupleCount() const {
  return tile_group_header->GetActiveTupleCount();
}

//===--------------------------------------------------------------------===//
// Operations
//===--------------------------------------------------------------------===//

/**
 * Grab next slot (thread-safe) and fill in the tuple
 *
 * Returns slot where inserted (INVALID_ID if not inserted)
 */
oid_t TileGroup::InsertTuple(txn_id_t transaction_id, const Tuple *tuple) {
  oid_t tuple_slot_id = tile_group_header->GetNextEmptyTupleSlot();

  LOG_TRACE("Tile Group Id :: %lu status :: %lu out of %lu slots \n",
            tile_group_id, tuple_slot_id, num_tuple_slots);

  // No more slots
  if (tuple_slot_id == INVALID_OID){
    LOG_INFO("Failed to get next empty tuple slot within tile group.");
    return INVALID_OID;
  }

  //tile_group_header->LatchTupleSlot(tuple_slot_id, transaction_id);

  oid_t tile_column_count;
  oid_t column_itr = 0;

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    const catalog::Schema &schema = tile_schemas[tile_itr];
    tile_column_count = schema.GetColumnCount();

    storage::Tile *tile = GetTile(tile_itr);
    assert(tile);
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    assert(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    storage::Tuple tile_tuple(&schema, tile_tuple_location);

    for (oid_t tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      tile_tuple.SetValueAllocate(tile_column_itr, tuple->GetValue(column_itr),
                                  tile->GetPool());
      column_itr++;
    }
  }

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_slot_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_slot_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_slot_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_slot_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_slot_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_slot_id, MAX_CID);
  tile_group_header->SetInsertCommit(tuple_slot_id, false);
  tile_group_header->SetDeleteCommit(tuple_slot_id, false);

  return tuple_slot_id;
}


/**
 * Grab specific slot and fill in the tuple
 * Used by recovery
 * Returns slot where inserted (INVALID_ID if not inserted)
 */
oid_t TileGroup::InsertTuple(txn_id_t transaction_id, oid_t tuple_slot_id, const Tuple *tuple) {
  auto status = tile_group_header->GetEmptyTupleSlot(tuple_slot_id);

  // No more slots
  if (status == false) return INVALID_OID;

  LOG_TRACE("Tile Group Id :: %lu status :: %lu out of %lu slots \n",
            tile_group_id, tuple_slot_id, num_tuple_slots);

  oid_t tile_column_count;
  oid_t column_itr = 0;

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    const catalog::Schema &schema = tile_schemas[tile_itr];
    tile_column_count = schema.GetColumnCount();

    storage::Tile *tile = GetTile(tile_itr);
    assert(tile);
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    assert(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    storage::Tuple tile_tuple(&schema, tile_tuple_location);

    for (oid_t tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      tile_tuple.SetValueAllocate(tile_column_itr, tuple->GetValue(column_itr),
                                  tile->GetPool());
      column_itr++;
    }
  }

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_slot_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_slot_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_slot_id, MAX_CID);
  tile_group_header->SetInsertCommit(tuple_slot_id, false);
  tile_group_header->SetDeleteCommit(tuple_slot_id, false);
  tile_group_header->SetPrevItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);

  return tuple_slot_id;
}



Tuple *TileGroup::SelectTuple(oid_t tile_offset, oid_t tuple_slot_id) {
  assert(tile_offset < tile_count);
  assert(tuple_slot_id < num_tuple_slots);

  // is it within bounds ?
  if (tuple_slot_id >= GetNextTupleSlot()) return nullptr;

  Tile *tile = GetTile(tile_offset);
  assert(tile);
  Tuple *tuple = tile->GetTuple(tuple_slot_id);
  return tuple;
}

Tuple *TileGroup::SelectTuple(oid_t tuple_slot_id) {
  // is it within bounds ?
  if (tuple_slot_id >= GetNextTupleSlot()) return nullptr;

  // allocate a new copy of the original tuple
  Tuple *tuple = new Tuple(table->GetSchema(), true);
  oid_t tuple_attr_itr = 0;

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    Tile *tile = GetTile(tile_itr);
    assert(tile);

    // tile tuple wrapper
    Tuple tile_tuple(tile->GetSchema(), tile->GetTupleLocation(tuple_slot_id));
    oid_t tile_tuple_count = tile->GetColumnCount();

    for (oid_t tile_tuple_attr_itr = 0; tile_tuple_attr_itr < tile_tuple_count;
         tile_tuple_attr_itr++) {
      Value val = tile_tuple.GetValue(tile_tuple_attr_itr);
      tuple->SetValueAllocate(tuple_attr_itr++, val, nullptr);
    }
  }

  return tuple;
}

// delete tuple at given slot if it is neither already locked nor deleted in future.
bool TileGroup::DeleteTuple(txn_id_t transaction_id, oid_t tuple_slot_id, cid_t last_cid) {

  // do a dirty delete
  if (tile_group_header->LatchTupleSlot(tuple_slot_id, transaction_id)) {
    if (tile_group_header->IsDeletable(tuple_slot_id, transaction_id, last_cid)) {
      return true;
    } else {
      LOG_INFO("Delete failed: not deletable");
      tile_group_header->ReleaseTupleSlot(tuple_slot_id, transaction_id);
      return false;
    }
  } else if (tile_group_header->GetTransactionId(tuple_slot_id) == transaction_id) {
    // is a own insert, is already latched by myself and is safe to set
    LOG_INFO("is this a own insert? txn_id = %lu, cbeg = %lu, cend = %lu", tile_group_header->GetTransactionId(tuple_slot_id),
             tile_group_header->GetBeginCommitId(tuple_slot_id),
             tile_group_header->GetEndCommitId(tuple_slot_id));
    assert(tile_group_header->GetBeginCommitId(tuple_slot_id) == MAX_CID);
    assert(tile_group_header->GetEndCommitId(tuple_slot_id) == MAX_CID);
    tile_group_header->SetTransactionId(tuple_slot_id, INVALID_TXN_ID);
    return true;
  } else {
    LOG_INFO("Delete failed: Latch failed and Ownership check failed: %lu != %lu",
             tile_group_header->GetTransactionId(tuple_slot_id), transaction_id);
    return false;
  }
}

void TileGroup::CommitInsertedTuple(oid_t tuple_slot_id,
                                    txn_id_t transaction_id, cid_t commit_id) {
  // set the begin commit id to persist insert
  if (tile_group_header->ReleaseTupleSlot(tuple_slot_id, transaction_id)) {
    tile_group_header->SetBeginCommitId(tuple_slot_id, commit_id);
  }
}

void TileGroup::CommitDeletedTuple(oid_t tuple_slot_id, txn_id_t transaction_id,
                                   cid_t commit_id) {
  // set the end commit id to persist delete
  if (tile_group_header->ReleaseTupleSlot(tuple_slot_id, transaction_id)) {
    tile_group_header->SetEndCommitId(tuple_slot_id, commit_id);
  }
}
/**
 * It is either a insert or a self-deleted insert
 */
void TileGroup::AbortInsertedTuple(oid_t tuple_slot_id) {
  tile_group_header->SetTransactionId(tuple_slot_id, INVALID_TXN_ID);
  // undo insert (we don't reset MVCC info currently)
  //TODO: can reclaim tuple here
  //ReclaimTuple(tuple_slot_id);
}

void TileGroup::AbortDeletedTuple(oid_t tuple_slot_id, txn_id_t transaction_id) {
  // undo deletion
  tile_group_header->ReleaseTupleSlot(tuple_slot_id, transaction_id);
}

// Sets the tile id and column id w.r.t that tile corresponding to
// the specified tile group column id.
void TileGroup::LocateTileAndColumn(oid_t column_offset, oid_t &tile_offset,
                                    oid_t &tile_column_offset) {
  assert(column_map.count(column_offset) != 0);

  // get the entry in the column map
  auto entry = column_map.at(column_offset);
  tile_offset = entry.first;
  tile_column_offset = entry.second;
}

oid_t TileGroup::GetTileIdFromColumnId(oid_t column_id) {
  oid_t tile_column_id, tile_offset;
  LocateTileAndColumn(column_id, tile_offset, tile_column_id);
  return tile_offset;
}

oid_t TileGroup::GetTileColumnId(oid_t column_id) {
  oid_t tile_column_id, tile_offset;
  LocateTileAndColumn(column_id, tile_offset, tile_column_id);
  return tile_column_id;
}

Value TileGroup::GetValue(oid_t tuple_id, oid_t column_id) {
  assert(tuple_id < GetNextTupleSlot());
  oid_t tile_column_id, tile_offset;
  LocateTileAndColumn(column_id, tile_offset, tile_column_id);
  return GetTile(tile_offset)->GetValue(tuple_id, tile_column_id);
}

Tile *TileGroup::GetTile(const oid_t tile_offset) const {
  assert(tile_offset < tile_count);
  Tile *tile = tiles[tile_offset];
  return tile;
}

double TileGroup::GetSchemaDifference(const storage::column_map_type& new_column_map) {
  double theta = 0;
  size_t capacity = column_map.size();
  double diff = 0;

  for(oid_t col_itr = 0 ; col_itr < capacity ; col_itr++) {
    auto& old_col = column_map.at(col_itr);
    auto& new_col = new_column_map.at(col_itr);

    // The tile don't match
    if(old_col.first != new_col.first)
      diff++;
  }

  // compute diff
  theta = diff/capacity;

  return theta;
}

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

// Get a string representation of this tile group
std::ostream &operator<<(std::ostream &os, const TileGroup &tile_group) {
  os << "=============================================================\n";

  os << "TILE GROUP :\n";
  os << "\tCatalog ::"
     << " DB: " << tile_group.database_id << " Table: " << tile_group.table_id
     << " Tile Group:  " << tile_group.tile_group_id << "\n";

  os << " TILE GROUP HEADER :: " << tile_group.tile_group_header;

  os << "\tActive Tuples:  "
     << tile_group.tile_group_header->GetActiveTupleCount() << " out of "
     << tile_group.num_tuple_slots << " slots\n";

  for (oid_t tile_itr = 0; tile_itr < tile_group.tile_count; tile_itr++) {
    Tile *tile = tile_group.GetTile(tile_itr);
    if (tile != nullptr) os << (*tile);
  }

  auto header = tile_group.GetHeader();
  if (header != nullptr) os << (*header);

  os << "=============================================================\n";

  return os;
}

}  // End storage namespace
}  // End peloton namespace
