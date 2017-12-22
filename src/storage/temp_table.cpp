//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// temp_table.cpp
//
// Identification: /peloton/src/storage/temp_table.cpp
//
// Copyright (c) 2015, 2016 Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/temp_table.h"

#include <sstream>

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"

namespace peloton {
namespace storage {

TempTable::TempTable(const oid_t &table_oid, catalog::Schema *schema,
                     const bool own_schema,
                     const peloton::LayoutType layout_type)
    : AbstractTable(table_oid, schema, own_schema, layout_type) {
  // We only want to instantiate a single TileGroup
  AddDefaultTileGroup();
}

TempTable::~TempTable() {
  // Nothing to see, nothing to do
}

ItemPointer TempTable::InsertTuple(
    const Tuple *tuple, UNUSED_ATTRIBUTE concurrency::TransactionContext *transaction,
    UNUSED_ATTRIBUTE ItemPointer **index_entry_ptr) {
  PL_ASSERT(tuple != nullptr);
  PL_ASSERT(transaction == nullptr);
  PL_ASSERT(index_entry_ptr == nullptr);

  std::shared_ptr<storage::TileGroup> tile_group;
  oid_t tuple_slot = INVALID_OID;
  oid_t tile_group_id = INVALID_OID;

  for (int i = 0, cnt = (int)tile_groups_.size(); i < cnt; i++) {
    tile_group = tile_groups_[i];
    tuple_slot = tile_group->InsertTuple(tuple);

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      tile_group_id = tile_group->GetTileGroupId();
      LOG_TRACE("Inserted tuple into %s", GetName().c_str());
      break;
    }
  }
  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    AddDefaultTileGroup();
  }
  if (tile_group_id == INVALID_OID) {
    LOG_WARN("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  // Set tuple location and increase our counter
  ItemPointer location(tile_group_id, tuple_slot);
  IncreaseTupleCount(1);

  // Make sure that we mark the tuple as active in the TileGroupHeader too
  // If you don't do this, then anybody that tries to use a LogicalTile wrapper
  // on this TempTable will not have any active tuples.
  auto tile_group_header = tile_group->GetHeader();
  tile_group_header->SetTransactionId(location.offset, INITIAL_TXN_ID);

  return (location);
}
ItemPointer TempTable::InsertTuple(const Tuple *tuple) {
  return (this->InsertTuple(tuple, nullptr, nullptr));
}

std::shared_ptr<storage::TileGroup> TempTable::GetTileGroup(
    const std::size_t &tile_group_offset) const {
  PL_ASSERT(tile_group_offset < GetTileGroupCount());
  return (tile_groups_[tile_group_offset]);
}

std::shared_ptr<storage::TileGroup> TempTable::GetTileGroupById(
    const oid_t &tile_group_id) const {
  for (auto tg : tile_groups_) {
    if (tg->GetTileGroupId() == tile_group_id) {
      return (tg);
    }
  }
  LOG_WARN("No TileGroup with id %d exists in %s", tile_group_id,
           this->GetName().c_str());
  return (nullptr);
}

oid_t TempTable::AddDefaultTileGroup() {
  column_map_type column_map;

  // Well, a TempTable doesn't really care about TileGroupIds
  // And nobody else in the system should be referencing our boys directly,
  // so we're just going to use a simple counter for these ids
  // We need to do this because otherwise we will think that our inserts
  // failed all the time if use INVALID_OID
  oid_t tile_group_id =
      TEMPTABLE_TILEGROUP_ID + static_cast<int>(tile_groups_.size());

  // Figure out the partitioning for given tilegroup layout
  column_map =
      AbstractTable::GetTileGroupLayout();

  // Create a tile group with that partitioning
  std::shared_ptr<storage::TileGroup> tile_group(
      AbstractTable::GetTileGroupWithLayout(
          INVALID_OID, tile_group_id, column_map, TEMPTABLE_DEFAULT_SIZE));
  PL_ASSERT(tile_group.get());

  tile_groups_.push_back(tile_group);

  LOG_TRACE("Created TileGroup for %s\n%s\n", GetName().c_str(),
            tile_group->GetInfo().c_str());

  return tile_group_id;
}

std::string TempTable::GetName() const {
  std::ostringstream os;
  os << "TEMP_TABLE[" << table_oid << "]";
  return (os.str());
}

}  // namespace storage
}  // namespace peloton
