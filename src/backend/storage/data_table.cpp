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

#include "backend/storage/data_table.h"
#include "backend/storage/database.h"

#include "backend/common/exception.h"
#include "backend/index/index.h"
#include "backend/common/logger.h"

#include <mutex>

namespace peloton {
namespace storage {

DataTable::DataTable(catalog::Schema *schema,
                     AbstractBackend *backend,
                     std::string table_name,
                     oid_t table_oid,
                     size_t tuples_per_tilegroup)
: AbstractTable(table_oid, table_name, schema),
  backend(backend),
  tuples_per_tilegroup(tuples_per_tilegroup) {

  // Create a tile group.
  AddDefaultTileGroup();

}

DataTable::~DataTable() {

  // clean up tile groups
  oid_t tile_group_count = GetTileGroupCount();
  for(oid_t tile_group_itr = 0 ; tile_group_itr < tile_group_count; tile_group_itr++){
    auto tile_group = GetTileGroup(tile_group_itr);
    delete tile_group;
  }

  // clean up indices
  for (auto index : indexes) {
    delete index;
  }

  // clean up foreign keys
  for (auto foreign_key : foreign_keys) {
    delete foreign_key;
  }

  // table owns its backend
  // TODO: Should we really be doing this here?
  delete backend;

  // AbstractTable cleans up the schema
}

//===--------------------------------------------------------------------===//
// TUPLE OPERATIONS
//===--------------------------------------------------------------------===//

ItemPointer DataTable::InsertTuple(txn_id_t transaction_id,
                                   const storage::Tuple *tuple,
                                   bool update) {

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

  // Index checks and updates
  if (update == false) {
    if (TryInsertInIndexes(tuple, location) == false) {
      tile_group->ReclaimTuple(tuple_slot);
      LOG_WARN("Index constraint violated : %s\n", tuple->GetInfo().c_str());
      return INVALID_ITEMPOINTER;
    }
  }
  else {
    // just do a blind insert
    InsertInIndexes(tuple, location);
    return location;
  }

  return location;
}

void DataTable::InsertInIndexes(const storage::Tuple *tuple, ItemPointer location) {
  for (auto index : indexes) {
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    storage::Tuple *key = new storage::Tuple(index_schema, true);
    key->SetFromTuple(tuple, indexed_columns);

    if (index->InsertEntry(key, location) == false) {
      location = INVALID_ITEMPOINTER;
      LOG_ERROR("Failed to insert key into index : %s \n",  key->GetInfo().c_str());
      delete key;
      break;
    }

    delete key;
  }
}

bool DataTable::TryInsertInIndexes(const storage::Tuple *tuple, ItemPointer location) {

  int index_count = GetIndexCount();
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    storage::Tuple *key = new storage::Tuple(index_schema, true);
    key->SetFromTuple(tuple, indexed_columns);

    // No need to check if it does not have unique keys
    if(index->HasUniqueKeys() == false) {
      bool status = index->InsertEntry(key, location);
      if (status == true) {
        delete key;
        continue;
      }
    }

    // Check if key already exists
    if (index->Exists(key) == true) {
      LOG_ERROR("Failed to insert into index %s.%s [%s]",
                GetName().c_str(), index->GetName().c_str(),
                index->GetTypeName().c_str());
    }
    else {
      bool status = index->InsertEntry(key, location);
      if (status == true) {
        delete key;
        continue;
      }
    }

    // Undo insert in other indexes as well
    for (int prev_index_itr = index_itr + 1; prev_index_itr < index_count; ++prev_index_itr) {
      auto prev_index = GetIndex(prev_index_itr);
      prev_index->DeleteEntry(key);
    }

    delete key;
    return false;
  }

  return true;
}

void DataTable::DeleteInIndexes(const storage::Tuple *tuple) {
  for (auto index : indexes) {
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    storage::Tuple *key = new storage::Tuple(index_schema, true);
    key->SetFromTuple(tuple, indexed_columns);

    if (index->DeleteEntry(key) == false) {
      delete key;
      LOG_WARN("Failed to delete tuple from index %s . %s %s",
               GetName().c_str(), index->GetName().c_str(),
               index->GetTypeName().c_str());
    }

    delete key;
  }

}

bool DataTable::CheckNulls(const storage::Tuple *tuple) const {
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


//===--------------------------------------------------------------------===//
// TILE GROUP
//===--------------------------------------------------------------------===//

oid_t DataTable::AddDefaultTileGroup() {
  oid_t tile_group_id = INVALID_OID;

  std::vector<catalog::Schema> schemas;
  std::vector<std::vector<std::string> > column_names;

  tile_group_id = catalog::Manager::GetInstance().GetNextOid();
  schemas.push_back(*schema);

  TileGroup* tile_group = TileGroupFactory::GetTileGroup(database_oid, table_oid, tile_group_id,
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

void DataTable::AddTileGroup(TileGroup *tile_group) {

  {
    std::lock_guard<std::mutex> lock(table_mutex);

    tile_groups.push_back(tile_group->GetTileGroupId());
    oid_t tile_group_id = tile_group->GetTileGroupId();

    // add tile group metadata in locator
    catalog::Manager::GetInstance().SetLocation(tile_group_id, tile_group);
    LOG_TRACE("Recording tile group : %d \n", tile_group_id);
  }

}


size_t DataTable::GetTileGroupCount() const {
  size_t size = tile_groups.size();
  return size;
}

TileGroup *DataTable::GetTileGroup(oid_t tile_group_id) const {
  assert(tile_group_id < GetTileGroupCount());

  auto& manager = catalog::Manager::GetInstance();
  storage::TileGroup *tile_group = static_cast<storage::TileGroup *>(manager.GetLocation(tile_groups[tile_group_id]));
  assert(tile_group);

  return tile_group;
}


std::ostream& operator<<(std::ostream& os, const DataTable& table) {
  os << "=====================================================\n";
  os << "TABLE :\n";

  oid_t tile_group_count = table.GetTileGroupCount();
  os << "Tile Group Count : " << tile_group_count << "\n";

  oid_t tuple_count = 0;
  for (oid_t tile_group_itr = 0 ; tile_group_itr < tile_group_count ; tile_group_itr++) {
    auto tile_group = table.GetTileGroup(tile_group_itr);
    auto tile_tuple_count = tile_group->GetNextTupleSlot();

    os << "Tile Group Id  : " << tile_group_itr << " Tuple Count : " << tile_tuple_count << "\n";
    os << (*tile_group);

    tuple_count += tile_tuple_count;
  }

  os << "Table Tuple Count :: " << tuple_count << "\n";

  os << "=====================================================\n";

  return os;
}

//===--------------------------------------------------------------------===//
// INDEX
//===--------------------------------------------------------------------===//

void DataTable::AddIndex(index::Index *index) {
  {
    std::lock_guard<std::mutex> lock(table_mutex);
    indexes.push_back(index);
  }

  // Update index stats
  auto index_type = index->GetIndexType();
  if(index_type == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
    has_primary_key = true;
  }
  else if(index_type == INDEX_CONSTRAINT_TYPE_UNIQUE) {
    unique_constraint_count++;
  }

}

index::Index* DataTable::GetIndexWithOid(const oid_t index_oid) const {
  for(auto index : indexes)
    if(index->GetOid() == index_oid)
      return index;

  return nullptr;
}

void DataTable::DropIndexWithOid(const oid_t index_id) {
  {
    std::lock_guard<std::mutex> lock(table_mutex);

    oid_t index_offset = 0;
    for(auto index : indexes) {
      if(index->GetOid() == index_id)
        break;
      index_offset++;
    }
    assert(index_offset < indexes.size());

    // Drop the index
    indexes.erase(indexes.begin() + index_offset);
  }
}

index::Index *DataTable::GetIndex(const oid_t index_offset) const {
  assert(index_offset < indexes.size());
  auto index = indexes.at(index_offset);
  return index;
}

oid_t DataTable::GetIndexCount() const {
  return indexes.size();
}

//===--------------------------------------------------------------------===//
// FOREIGN KEYS
//===--------------------------------------------------------------------===//

void DataTable::AddForeignKey(catalog::ForeignKey *key) {
  {
    std::lock_guard<std::mutex> lock(table_mutex);
    catalog::Schema * schema = this->GetSchema();
    catalog::Constraint constraint( CONSTRAINT_TYPE_FOREIGN, key->GetConstraintName());
    constraint.SetForeignKeyListOffset(GetForeignKeyCount());
    for( auto fk_column : key->GetFKColumnNames()){
      schema->AddConstraint(fk_column, constraint);
    }
    //TODO :: We need this one..
    catalog::ForeignKey* fk = new catalog::ForeignKey( *key );
    foreign_keys.push_back(fk);
  }
}

catalog::ForeignKey *DataTable::GetForeignKey(const oid_t key_offset) const {
  catalog::ForeignKey *key = nullptr;
  key = foreign_keys.at(key_offset);
  return key;
}

void DataTable::DropForeignKey(const oid_t key_offset) {
  {
    std::lock_guard<std::mutex> lock(table_mutex);
    assert(key_offset < foreign_keys.size());
    foreign_keys.erase(foreign_keys.begin() + key_offset);
  }
}

oid_t DataTable::GetForeignKeyCount() const {
  return foreign_keys.size();
}


} // End storage namespace
} // End peloton namespace

