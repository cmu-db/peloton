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
#include "catalog/manager.h"

namespace nstore {
namespace storage {

Table::Table(catalog::Schema *schema,
             Backend *backend,
             std::string table_name)
: database_id(INVALID_ID),
  table_id(INVALID_ID),
  backend(backend),
  schema(schema),
  table_name(table_name){

  // Create a tile group.
  AddDefaultTileGroup();

}

Table::~Table() {

  // clean up indices
  for (auto index : indexes) {
    delete index;
  }

  // clean up tile groups
  for(auto tile_group : tile_groups)
    delete tile_group;

  // table owns its backend
  delete backend;

  // clean up schema
  delete schema;
}

oid_t Table::AddDefaultTileGroup() {
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

    // A) no tile groups in table
    if(tile_groups.empty()) {
      LOG_TRACE("Added first tile group \n");
      tile_groups.push_back(tile_group);
      // add tile group metadata in locator
      catalog::Manager::GetInstance().SetLocation(tile_group_id, tile_group);
      LOG_TRACE("Recording tile group : %d \n", tile_group_id);
      return tile_group_id;
    }

    // A) no slots in last tile group in table
    auto last_tile_group = tile_groups.back();
    id_t active_tuple_count = last_tile_group->GetNextTupleSlot();
    id_t allocated_tuple_count = last_tile_group->GetAllocatedTupleCount();
    if( active_tuple_count < allocated_tuple_count) {
      LOG_TRACE("Slot exists in last tile group :: %lu %lu \n", active_tuple_count, allocated_tuple_count);
      delete tile_group;
      return INVALID_OID;
    }

    LOG_TRACE("Added a tile group \n");
    tile_groups.push_back(tile_group);
    // add tile group metadata in locator
    catalog::Manager::GetInstance().SetLocation(tile_group_id, tile_group);
    LOG_TRACE("Recording tile group : %d \n", tile_group_id);
  }

  return tile_group_id;
}

void Table::AddTileGroup(TileGroup *tile_group) {

  {
    std::lock_guard<std::mutex> lock(table_mutex);

    tile_groups.push_back(tile_group);
    oid_t tile_group_id = tile_group->GetTileGroupId();

    // add tile group metadata in locator
    catalog::Manager::GetInstance().SetLocation(tile_group_id, tile_group);
    LOG_TRACE("Recording tile group : %d \n", tile_group_id);
  }

}

void Table::AddIndex(index::Index *index) {

  {
    std::lock_guard<std::mutex> lock(table_mutex);
    indexes.push_back(index);
  }

}

ItemPointer Table::InsertTuple(txn_id_t transaction_id, const storage::Tuple *tuple, bool update){
  assert(tuple);

  // Not NULL checks
  if(CheckNulls(tuple) == false){
    throw ConstraintException("Not NULL constraint violated : " + tuple->GetInfo());
    return ItemPointer();
  }

  // Actual insertion into last tile group
  TileGroup *tile_group = nullptr;
  id_t tuple_slot = INVALID_ID;

  while(tuple_slot == INVALID_ID){
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      tile_group = tile_groups.back();
    }

    tuple_slot = tile_group->InsertTuple(transaction_id, tuple);
    if(tuple_slot == INVALID_ID)
      AddDefaultTileGroup();
  }

  // Index checks
  ItemPointer location = ItemPointer(tile_group->GetTileGroupId(), tuple_slot);
  if(update == false){
    if(TryInsertInIndexes(tuple, location) == false){
      tile_group->ReclaimTuple(tuple_slot);
      throw ConstraintException("Index constraint violated : " + tuple->GetInfo());
      return location;
    }
  }
  else{
    // just do a blind insert
    InsertInIndexes(tuple, location);
    return location;
  }

  return location;
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

  os << "=====================================================\n";
  os << "TABLE :\n";

  id_t tile_group_count = table.GetTileGroupCount();
  std::cout << "Tile Group Count : " << tile_group_count << "\n";

  id_t tuple_count = 0;
  for(id_t tile_group_itr = 0 ; tile_group_itr < tile_group_count ; tile_group_itr++) {
    auto tile_group = table.GetTileGroup(tile_group_itr);
    auto tile_tuple_count = tile_group->GetNextTupleSlot();

    std::cout << "Tile Group Id  : " << tile_group_itr << " Tuple Count : " << tile_tuple_count << "\n";
    std::cout << (*tile_group);

    tuple_count += tile_tuple_count;
  }

  std::cout << "Table Tuple Count :: " << tuple_count << "\n";

  os << "=====================================================\n";

  return os;
}

} // End storage namespace
} // End nstore namespace

