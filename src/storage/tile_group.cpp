//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.cpp
//
// Identification: src/storage/tile_group.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/tile_group.h"

#include <numeric>

#include "common/platform.h"
#include "catalog/manager.h"
#include "common/logger.h"
#include "common/types.h"
#include "storage/abstract_table.h"
#include "storage/tile.h"
#include "storage/tuple.h"
#include "storage/tile_group_header.h"
#include "storage/rollback_segment.h"

namespace peloton {
namespace storage {

TileGroup::TileGroup(BackendType backend_type,
                     TileGroupHeader *tile_group_header, AbstractTable *table,
                     const std::vector<catalog::Schema> &schemas,
                     const column_map_type &column_map, int tuple_count)
    : database_id(INVALID_OID),
      table_id(INVALID_OID),
      tile_group_id(INVALID_OID),
      backend_type(backend_type),
      tile_schemas(schemas),
      tile_group_header(tile_group_header),
      table(table),
      num_tuple_slots(tuple_count),
      column_map(column_map) {
  tile_count = tile_schemas.size();

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    auto &manager = catalog::Manager::GetInstance();
    oid_t tile_id = manager.GetNextOid();

    std::shared_ptr<Tile> tile(storage::TileFactory::GetTile(
        backend_type, database_id, table_id, tile_group_id, tile_id,
        tile_group_header, tile_schemas[tile_itr], this, tuple_count));

    // Add a reference to the tile in the tile group
    tiles.push_back(tile);
  }
}

TileGroup::~TileGroup() {
  // Drop references on all tiles

  // clean up tile group header
  delete tile_group_header;
}

oid_t TileGroup::GetTileId(const oid_t tile_id) const {
  PL_ASSERT(tiles[tile_id]);
  return tiles[tile_id]->GetTileId();
}

peloton::VarlenPool *TileGroup::GetTilePool(const oid_t tile_id) const {
  Tile *tile = GetTile(tile_id);

  if (tile != nullptr) return tile->GetPool();

  return nullptr;
}

// TODO: check when this function is called. --Yingjun
oid_t TileGroup::GetNextTupleSlot() const {
  return tile_group_header->GetCurrentNextTupleSlot();
}

// this function is called only when building tile groups for aggregation
// operations.
oid_t TileGroup::GetActiveTupleCount() const {
  return tile_group_header->GetActiveTupleCount();
}

//===--------------------------------------------------------------------===//
// Operations
//===--------------------------------------------------------------------===//

/**
 * Apply the column delta on the rollback segment to the given tuple
 */
void TileGroup::ApplyRollbackSegment(char *rb_seg, const oid_t &tuple_slot_id) {

  auto seg_col_count = storage::RollbackSegmentPool::GetColCount(rb_seg);
  auto table_schema = GetAbstractTable()->GetSchema();

  for (size_t idx = 0; idx < seg_col_count; ++idx) {
    auto col_id = storage::RollbackSegmentPool::GetIdOffsetPair(rb_seg, idx)->col_id;
    Value col_value = storage::RollbackSegmentPool::GetValue(rb_seg, table_schema, idx);

    // Get target tile
    auto tile_id = GetTileIdFromColumnId(col_id);
    PL_ASSERT(tile_id < GetTileCount());
    storage::Tile *tile = GetTile(tile_id);
    PL_ASSERT(tile);

    // Get tile schema
    auto &tile_schema = tile_schemas[tile_id];

    // Get a tuple wrapper
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    PL_ASSERT(tile_tuple_location);
    storage::Tuple tile_tuple(&tile_schema, tile_tuple_location);

    // Write the value to tuple
    auto tile_col_idx = GetTileColumnId(col_id);
    tile_tuple.SetValue(tile_col_idx, col_value, tile->GetPool());
  }
}


/**
 * Grab next slot (thread-safe) and fill in the tuple
 *
 * Returns slot where inserted (INVALID_ID if not inserted)
 */
void TileGroup::CopyTuple(const Tuple *tuple, const oid_t &tuple_slot_id) {
  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ",
            tile_group_id, tuple_slot_id, num_tuple_slots);

  oid_t tile_column_count;
  oid_t column_itr = 0;

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    const catalog::Schema &schema = tile_schemas[tile_itr];
    tile_column_count = schema.GetColumnCount();

    storage::Tile *tile = GetTile(tile_itr);
    PL_ASSERT(tile);
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    PL_ASSERT(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    storage::Tuple tile_tuple(&schema, tile_tuple_location);

    for (oid_t tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      tile_tuple.SetValue(tile_column_itr, tuple->GetValue(column_itr),
                          tile->GetPool());
      column_itr++;
    }
  }
}

// This is commented out before merge
void TileGroup::CopyTuple(const oid_t &tuple_slot_id, Tuple *tuple) {
  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ",
            tile_group_id, tuple_slot_id, num_tuple_slots);
  auto schema = table->GetSchema();

  PL_ASSERT(tuple->GetColumnCount() == schema->GetColumnCount());

  for (oid_t col_id = 0; col_id < schema->GetColumnCount(); ++col_id) {
    tuple->SetValue(col_id, GetValue(tuple_slot_id, col_id), nullptr);
  }
}

/**
 * Grab next slot (thread-safe) and fill in the tuple
 *
 * Returns slot where inserted (INVALID_ID if not inserted)
 */
oid_t TileGroup::InsertTuple(const Tuple *tuple) {
  oid_t tuple_slot_id = tile_group_header->GetNextEmptyTupleSlot();

  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ",
            tile_group_id, tuple_slot_id, num_tuple_slots);

  // No more slots
  if (tuple_slot_id == INVALID_OID) {
    LOG_TRACE("Failed to get next empty tuple slot within tile group.");
    return INVALID_OID;
  }

  oid_t tile_column_count;
  oid_t column_itr = 0;

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    const catalog::Schema &schema = tile_schemas[tile_itr];
    tile_column_count = schema.GetColumnCount();

    storage::Tile *tile = GetTile(tile_itr);
    PL_ASSERT(tile);
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    PL_ASSERT(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    storage::Tuple tile_tuple(&schema, tile_tuple_location);

    for (oid_t tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      tile_tuple.SetValue(tile_column_itr, tuple->GetValue(column_itr),
                          tile->GetPool());
      column_itr++;
    }
  }


  // Set MVCC info
  PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot_id) == INVALID_TXN_ID);
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_slot_id) == MAX_CID);
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_slot_id) == MAX_CID);

  return tuple_slot_id;
}

/**
 * Grab specific slot and fill in the tuple
 * Used by recovery
 * Returns slot where inserted (INVALID_ID if not inserted)
 */
oid_t TileGroup::InsertTupleFromRecovery(cid_t commit_id, oid_t tuple_slot_id,
                                         const Tuple *tuple) {
  auto status = tile_group_header->GetEmptyTupleSlot(tuple_slot_id);

  // No more slots
  if (status == false) return INVALID_OID;

  tile_group_header->GetHeaderLock().Lock();

  cid_t current_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);
  if (current_begin_cid != MAX_CID && current_begin_cid > commit_id) {
    tile_group_header->GetHeaderLock().Unlock();
    return tuple_slot_id;
  }

  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ",
            tile_group_id, tuple_slot_id, num_tuple_slots);

  oid_t tile_column_count;
  oid_t column_itr = 0;

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    const catalog::Schema &schema = tile_schemas[tile_itr];
    tile_column_count = schema.GetColumnCount();

    storage::Tile *tile = GetTile(tile_itr);
    PL_ASSERT(tile);
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    PL_ASSERT(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    storage::Tuple tile_tuple(&schema, tile_tuple_location);

    for (oid_t tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      tile_tuple.SetValue(tile_column_itr, tuple->GetValue(column_itr),
                          tile->GetPool());
      column_itr++;
    }
  }

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_slot_id, INITIAL_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetEndCommitId(tuple_slot_id, MAX_CID);
  tile_group_header->SetInsertCommit(tuple_slot_id, false);
  tile_group_header->SetDeleteCommit(tuple_slot_id, false);
  tile_group_header->SetNextItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);

  tile_group_header->GetHeaderLock().Unlock();

  return tuple_slot_id;
}

oid_t TileGroup::DeleteTupleFromRecovery(cid_t commit_id, oid_t tuple_slot_id) {

  auto status = tile_group_header->GetEmptyTupleSlot(tuple_slot_id);

  tile_group_header->GetHeaderLock().Lock();

  cid_t current_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);
  if (current_begin_cid != MAX_CID && current_begin_cid > commit_id) {
    tile_group_header->GetHeaderLock().Unlock();
    return tuple_slot_id;
  }
  // No more slots
  if (status == false) {
    tile_group_header->GetHeaderLock().Unlock();
    return INVALID_OID;
  }
  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_slot_id, INVALID_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetEndCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetInsertCommit(tuple_slot_id, false);
  tile_group_header->SetDeleteCommit(tuple_slot_id, false);
  tile_group_header->SetNextItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);
  tile_group_header->GetHeaderLock().Unlock();
  return tuple_slot_id;
}

oid_t TileGroup::UpdateTupleFromRecovery(cid_t commit_id, oid_t tuple_slot_id,
                                         ItemPointer new_location) {
  auto status = tile_group_header->GetEmptyTupleSlot(tuple_slot_id);

  tile_group_header->GetHeaderLock().Lock();

  cid_t current_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);
  if (current_begin_cid != MAX_CID && current_begin_cid > commit_id) {
    tile_group_header->GetHeaderLock().Unlock();
    return tuple_slot_id;
  }

  // No more slots
  if (status == false) {
    tile_group_header->GetHeaderLock().Unlock();
    return INVALID_OID;
  }
  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_slot_id, INVALID_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetEndCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetInsertCommit(tuple_slot_id, false);
  tile_group_header->SetDeleteCommit(tuple_slot_id, false);
  tile_group_header->SetNextItemPointer(tuple_slot_id, new_location);
  tile_group_header->GetHeaderLock().Unlock();
  return tuple_slot_id;
}

/**
 * Grab specific slot and fill in the tuple
 * Used by checkpoint recovery
 * Returns slot where inserted (INVALID_ID if not inserted)
 */
oid_t TileGroup::InsertTupleFromCheckpoint(oid_t tuple_slot_id,
                                           const Tuple *tuple,
                                           cid_t commit_id) {
  auto status = tile_group_header->GetEmptyTupleSlot(tuple_slot_id);

  // No more slots
  if (status == false) return INVALID_OID;

  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ",
            tile_group_id, tuple_slot_id, num_tuple_slots);

  oid_t tile_column_count;
  oid_t column_itr = 0;

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    const catalog::Schema &schema = tile_schemas[tile_itr];
    tile_column_count = schema.GetColumnCount();

    storage::Tile *tile = GetTile(tile_itr);
    PL_ASSERT(tile);
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    PL_ASSERT(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    storage::Tuple tile_tuple(&schema, tile_tuple_location);

    for (oid_t tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      tile_tuple.SetValue(tile_column_itr, tuple->GetValue(column_itr),
                          tile->GetPool());
      column_itr++;
    }
  }

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_slot_id, INITIAL_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetEndCommitId(tuple_slot_id, MAX_CID);
  tile_group_header->SetInsertCommit(tuple_slot_id, false);
  tile_group_header->SetDeleteCommit(tuple_slot_id, false);
  tile_group_header->SetNextItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);

  return tuple_slot_id;
}

// Sets the tile id and column id w.r.t that tile corresponding to
// the specified tile group column id.
void TileGroup::LocateTileAndColumn(oid_t column_offset, oid_t &tile_offset,
                                    oid_t &tile_column_offset) {
  PL_ASSERT(column_map.count(column_offset) != 0);

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
  PL_ASSERT(tuple_id < GetNextTupleSlot());
  oid_t tile_column_id, tile_offset;
  LocateTileAndColumn(column_id, tile_offset, tile_column_id);
  return GetTile(tile_offset)->GetValue(tuple_id, tile_column_id);
}

Tile *TileGroup::GetTile(const oid_t tile_offset) const {
  PL_ASSERT(tile_offset < tile_count);
  Tile *tile = tiles[tile_offset].get();
  return tile;
}

std::shared_ptr<Tile> TileGroup::GetTileReference(
    const oid_t tile_offset) const {
  PL_ASSERT(tile_offset < tile_count);
  return tiles[tile_offset];
}

double TileGroup::GetSchemaDifference(
    const storage::column_map_type &new_column_map) {
  double theta = 0;
  size_t capacity = column_map.size();
  double diff = 0;

  for (oid_t col_itr = 0; col_itr < capacity; col_itr++) {
    auto &old_col = column_map.at(col_itr);
    auto &new_col = new_column_map.at(col_itr);

    // The tile don't match
    if (old_col.first != new_col.first) diff++;
  }

  // compute diff
  theta = diff / capacity;

  return theta;
}

void TileGroup::Sync() {
  // Sync the tile group data by syncing all the underlying tiles
  for (auto tile : tiles) {
    tile->Sync();
  }
}

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

const std::string TileGroup::GetInfo() const {
  std::ostringstream os;

  os << "=============================================================\n";

  os << "TILE GROUP :\n";
  os << "\tCatalog ::"
     << " DB: " << database_id << " Table: " << table_id
     << " Tile Group:  " << tile_group_id << "\n";

  os << " TILE GROUP HEADER :: " << tile_group_header;

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    Tile *tile = GetTile(tile_itr);
    if (tile != nullptr) os << (*tile);
  }

  auto header = GetHeader();
  if (header != nullptr) os << (*header);

  os << "=============================================================\n";

  return os.str().c_str();
}

}  // End storage namespace
}  // End peloton namespace
