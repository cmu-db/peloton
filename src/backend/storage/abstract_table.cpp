/*-------------------------------------------------------------------------
 *
 * abstract_table.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/storage/abstract_table.h"

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tile_group_factory.h"

#include <mutex>

namespace nstore {
namespace storage {

AbstractTable::AbstractTable(catalog::Schema *schema,
             AbstractBackend *backend,
            size_t tuples_per_tilegroup)

: database_id(INVALID_OID), // FIXME
  table_id(INVALID_OID),    // FIXME
  backend(backend),
  schema(schema),
  tuples_per_tilegroup(tuples_per_tilegroup){

  // Create a tile group.
  AddDefaultTileGroup();

}

AbstractTable::~AbstractTable() {

  // clean up tile groups
  oid_t tile_group_count = GetTileGroupCount();
  for(oid_t tile_group_itr = 0 ; tile_group_itr < tile_group_count; tile_group_itr++){
    auto tile_group = GetTileGroup(tile_group_itr);
    delete tile_group;
  }

  // table owns its backend
  // TODO: Should we really be doing this here?
  delete backend;

  // clean up schema
  delete schema;
}

oid_t AbstractTable::AddDefaultTileGroup() {
  oid_t tile_group_id = INVALID_OID;

  std::vector<catalog::Schema> schemas;
  std::vector<std::vector<std::string> > column_names;

  tile_group_id = catalog::Manager::GetInstance().GetNextOid();
  schemas.push_back(*schema);

  TileGroup* tile_group = TileGroupFactory::GetTileGroup(database_id, table_id, tile_group_id,
                                                         this, backend,
                                                         schemas, tuples_per_tilegroup);

  LOG_TRACE("Trying to add a tile group \n");
  {
    std::lock_guard<std::mutex> lock(table_mutex);

    // Check if we actually need to allocate a tile group

    // (A) no tile groups in table
    if (tile_groups.empty()) {
      LOG_TRACE("Added first tile group \n");
      tile_groups.push_back(tile_group->GetTileGroupId());
      // add tile group metadata in locator
      catalog::Manager::GetInstance().SetLocation(tile_group_id, tile_group);
      LOG_TRACE("Recording tile group : %d \n", tile_group_id);
      return tile_group_id;
    }

    // (B) no slots in last tile group in table
    auto last_tile_group_offset = GetTileGroupCount() - 1;
    auto last_tile_group = GetTileGroup(last_tile_group_offset);

    oid_t active_tuple_count = last_tile_group->GetNextTupleSlot();
    oid_t allocated_tuple_count = last_tile_group->GetAllocatedTupleCount();
    if (active_tuple_count < allocated_tuple_count) {
      LOG_TRACE("Slot exists in last tile group :: %d %d \n", active_tuple_count, allocated_tuple_count);
      delete tile_group;
      return INVALID_OID;
    }

    LOG_TRACE("Added a tile group \n");
    tile_groups.push_back(tile_group->GetTileGroupId());

    // add tile group metadata in locator
    catalog::Manager::GetInstance().SetLocation(tile_group_id, tile_group);
    LOG_TRACE("Recording tile group : %d \n", tile_group_id);
  }

  return tile_group_id;
}

void AbstractTable::AddTileGroup(TileGroup *tile_group) {

  {
    std::lock_guard<std::mutex> lock(table_mutex);

    tile_groups.push_back(tile_group->GetTileGroupId());
    oid_t tile_group_id = tile_group->GetTileGroupId();

    // add tile group metadata in locator
    catalog::Manager::GetInstance().SetLocation(tile_group_id, tile_group);
    LOG_TRACE("Recording tile group : %d \n", tile_group_id);
  }

}

size_t AbstractTable::GetTileGroupCount() const {
  size_t size = tile_groups.size();
  return size;
}

TileGroup *AbstractTable::GetTileGroup(oid_t tile_group_id) const {
  assert(tile_group_id < GetTileGroupCount());

  auto& manager = catalog::Manager::GetInstance();
  storage::TileGroup *tile_group = static_cast<storage::TileGroup *>(manager.GetLocation(tile_groups[tile_group_id]));
  assert(tile_group);

  return tile_group;
}

ItemPointer AbstractTable::InsertTuple(txn_id_t transaction_id, const storage::Tuple *tuple) {
  assert(tuple);

  // (A) Not NULL checks
  if (CheckNulls(tuple) == false) {
    throw ConstraintException("Not NULL constraint violated : " + tuple->GetInfo());
    return ItemPointer();
  }

  TileGroup *tile_group = nullptr;
  oid_t tuple_slot = INVALID_OID;
  oid_t tile_group_offset = INVALID_OID;

  while (tuple_slot == INVALID_OID) {

    // (B) Figure out last tile group
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      tile_group_offset = GetTileGroupCount()-1;
      LOG_TRACE("Tile group offset :: %d \n", tile_group_offset);
    }

    // (C) Try to insert
    tile_group = GetTileGroup(tile_group_offset);
    tuple_slot = tile_group->InsertTuple(transaction_id, tuple);
    if (tuple_slot == INVALID_OID) {
      AddDefaultTileGroup();
    }

  }

  // Return Location
  ItemPointer location = ItemPointer(tile_group->GetTileGroupId(), tuple_slot);

  return location;
}


bool AbstractTable::CheckNulls(const storage::Tuple *tuple) const {
  assert (schema->GetColumnCount() == tuple->GetColumnCount());

  oid_t column_count = schema->GetColumnCount();
  for (int column_itr = column_count - 1; column_itr >= 0; --column_itr) {
    if (tuple->IsNull(column_itr) && schema->AllowNull(column_itr) == false) {
      LOG_TRACE ("%d th attribute in the tuple was NULL. It is non-nullable attribute.", column_itr);
      return false;
    }
  }

  return true;
}

std::ostream& operator<<(std::ostream& os, const AbstractTable& table) {

  os << "=====================================================\n";
  os << "TABLE :\n";

  oid_t tile_group_count = table.GetTileGroupCount();
  std::cout << "Tile Group Count : " << tile_group_count << "\n";

  oid_t tuple_count = 0;
  for (oid_t tile_group_itr = 0 ; tile_group_itr < tile_group_count ; tile_group_itr++) {
    auto tile_group = table.GetTileGroup(tile_group_itr);
    auto tile_tuple_count = tile_group->GetNextTupleSlot();

    std::cout << "Tile Group Id  : " << tile_group_itr;
    std::cout << " Tuple Count : " << tile_tuple_count << "\n";
    std::cout << (*tile_group);

    tuple_count += tile_tuple_count;
  }

  std::cout << "Table Tuple Count :: " << tuple_count << "\n";

  os << "=====================================================\n";

  return os;
}

} // End storage namespace
} // End nstore namespace

