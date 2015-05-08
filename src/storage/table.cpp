/*-------------------------------------------------------------------------
 *
 * table.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/table.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/table.h"

#include "common/exception.h"
#include "index/index.h"
#include "common/logger.h"

namespace nstore {
namespace storage {

oid_t Table::AddDefaultTileGroup() {
  oid_t tile_group_id = INVALID_OID;

  std::vector<catalog::Schema> schemas;
  std::vector<std::vector<std::string> > column_names;

  tile_group_id = catalog::Manager::GetInstance().GetNextOid();
  schemas.push_back(*schema);

  TileGroup* tile_group = TileGroupFactory::GetTileGroup(database_id, table_id, tile_group_id,
                                                         backend,
                                                         schemas, tuples_per_tilegroup);
  {
    std::lock_guard<std::mutex> lock(table_mutex);

    // Check if we actually need to allocate a tile group
    auto last_tile_group = tile_groups.back();
    if(last_tile_group->GetActiveTupleCount()
        < last_tile_group->GetAllocatedTupleCount()) {
      delete tile_group;
      return INVALID_OID;
    }

    tile_groups.push_back(tile_group);
  }

  return tile_group_id;
}

void Table::AddTileGroup(TileGroup *tile_group) {

  {
    std::lock_guard<std::mutex> lock(table_mutex);
    tile_groups.push_back(tile_group);
  }

}

void Table::AddIndex(index::Index *index) {

  {
    std::lock_guard<std::mutex> lock(table_mutex);
    indexes.push_back(index);
  }

}

id_t Table::InsertTuple(txn_id_t transaction_id, const storage::Tuple *tuple){
  assert(tuple);

  // Not NULL checks
  if(CheckNulls(tuple) == false){
    throw ConstraintException("Not NULL constraint violated : " + tuple->GetInfo());
    return INVALID_ID;
  }

  // Actual insertion into last tile group
  auto tile_group = tile_groups.back();

  // and try to insert into it
  id_t tuple_slot = tile_group->InsertTuple(transaction_id, tuple);

  // if that didn't work, need to add a new tile group
  if(tuple_slot == INVALID_ID){
    AddDefaultTileGroup();

    tile_group = tile_groups.back();
    tuple_slot = tile_group->InsertTuple(transaction_id, tuple);
    if(tuple_slot == INVALID_ID)
      return INVALID_ID;
  }

  // Index checks
  ItemPointer location = ItemPointer(tile_group->GetTileGroupId(), tuple_slot);
  if(TryInsertInIndexes(tuple, location) == false){
    tile_group->ReclaimTuple(tuple_slot);

    throw ConstraintException("Index constraint violated : " + tuple->GetInfo());
    return INVALID_ID;
  }

  return tuple_slot;
}

void Table::InsertInIndexes(const storage::Tuple *tuple, ItemPointer location) {
  for(auto index : indexes){
    if (index->InsertEntry(tuple, location) == false) {
      throw ExecutorException("Failed to insert tuple into index");
    }
  }
}

bool Table::TryInsertInIndexes(const storage::Tuple *tuple, ItemPointer location) {

  int index_count = GetIndexCount();
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {

    // No need to check if it does not have unique keys
    if(indexes[index_itr]->HasUniqueKeys() == false) {
      bool status = indexes[index_itr]->InsertEntry(tuple, location);
      if(status == true)
        continue;
    }

    // Check if key already exists
    if(indexes[index_itr]->Exists(tuple) == true) {
      LOG_ERROR("Failed to insert into index %s.%s [%s]",
                GetName().c_str(), indexes[index_itr]->GetName().c_str(),
                indexes[index_itr]->GetTypeName().c_str());
    }
    else {
      bool status = indexes[index_itr]->InsertEntry(tuple, location);
      if(status == true)
        continue;
    }

    // Undo insert in other indexes as well
    for (int prev_index_itr = index_itr + 1; prev_index_itr < index_count; ++prev_index_itr) {
      indexes[prev_index_itr]->DeleteEntry(tuple);
    }
    return false;
  }

  return true;
}

void Table::DeleteInIndexes(const storage::Tuple *tuple) {
  for(auto index : indexes){
    if (index->DeleteEntry(tuple) == false) {
      throw ExecutorException("Failed to delete tuple from index " +
                              GetName() + "." + index->GetName() + " " +index->GetTypeName());
    }
  }
}

bool Table::CheckNulls(const storage::Tuple *tuple) const {
  assert (schema->GetColumnCount() == tuple->GetColumnCount());

  id_t column_count = schema->GetColumnCount();
  for (int column_itr = column_count - 1; column_itr >= 0; --column_itr) {

    if (tuple->IsNull(column_itr) && schema->AllowNull(column_itr) == false) {
      LOG_TRACE ("%d th attribute in the tuple was NULL. It is non-nullable attribute.", column_itr);
      return false;
    }
  }

  return true;
}

std::ostream& operator<<(std::ostream& os, const Table& table){

  os << "================================================================================================\n";
  os << "TABLE :\n";

  id_t tile_group_count = table.GetTileGroupCount();
  std::cout << "Tile Group Count : " << tile_group_count << "\n";

  id_t tuple_count = 0;
  for(id_t tile_group_itr = 0 ; tile_group_itr < tile_group_count ; tile_group_itr++) {
    auto tile_tuple_count = table.GetTileGroup(tile_group_itr)->GetActiveTupleCount();
    std::cout << "Tile Group Id  : " << tile_group_itr << " Tuple Count : " << tile_tuple_count << "\n";
    tuple_count += tile_tuple_count;
  }

  std::cout << "Table Tuple Count :: " << tuple_count << "\n";

  os << "================================================================================================\n";

  return os;
}

} // End storage namespace
} // End nstore namespace

