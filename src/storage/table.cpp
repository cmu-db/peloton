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

  std::vector<catalog::Schema> schemas;
  std::vector<std::vector<std::string> > column_names;

  oid_t tile_group_id = catalog::Manager::GetInstance().GetNextOid();
  schemas.push_back(*schema);

  TileGroup* tile_group = TileGroupFactory::GetTileGroup(database_id, table_id, tile_group_id,
                                                         backend,
                                                         schemas, tuples_per_tilegroup);

  {
    std::lock_guard<std::mutex> lock(table_mutex);
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

  ///////////////////////////////////////////////////
  // Not NULL checks
  ///////////////////////////////////////////////////

  if(CheckNulls(tuple) == false){
    throw ConstraintException("Not NULL constraint violated : " + tuple->GetInfo());
    return INVALID_ID;
  }

  ///////////////////////////////////////////////////
  // Actual insertion
  ///////////////////////////////////////////////////

  // find last tile group
  auto tile_group = tile_groups.back();

  // and try to insert into it
  id_t tuple_id = tile_group->InsertTuple(transaction_id, tuple);

  // if that didn't work, need to add a new tile group
  if(tuple_id == INVALID_ID){

    // try to insert again to check if someone else added a tile group already
    tile_group = tile_groups.back();
    tuple_id = tile_group->InsertTuple(transaction_id, tuple);

    if(tuple_id == INVALID_ID) {
      AddDefaultTileGroup();
      LOG_INFO("Added tile group");

      // this must work
      tile_group = tile_groups.back();
      tuple_id = tile_group->InsertTuple(transaction_id, tuple);
      assert(tuple_id != INVALID_ID);
    }
  }

  ///////////////////////////////////////////////////
  // Index checks
  ///////////////////////////////////////////////////

  ItemPointer location(tile_group->GetTileGroupId(), tuple_id);
  if(TryInsertInIndexes(tuple, location) == false){
    throw ConstraintException("Index constraint violated : " + tuple->GetInfo());
    return INVALID_ID;
  }

  return tuple_id;
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

void Table::UpdateInIndexes(const storage::Tuple *tuple, ItemPointer location, ItemPointer old_location) {
  for(auto index : indexes){
    LOG_TRACE("Updating tuple address in index %s.%s [%s]",
              GetName().c_str(), index->GetName().c_str(), index->GetTypeName().c_str());

    if (index->UpdateEntry(tuple, location, old_location) == false) {
      LOG_ERROR("ERROR: Failed to update tuple to to new location ");
      throw ExecutorException("Failed to update tuple to new address in index "
          + GetName() + "." + index->GetName() + " " +index->GetTypeName());
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

  os << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
  os << "TABLE :\n";

  for(auto tile_group : table.tile_groups)
    std::cout << (*tile_group);

  os << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";

  return os;
}

} // End storage namespace
} // End nstore namespace

