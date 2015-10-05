//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// data_table.cpp
//
// Identification: src/backend/storage/data_table.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <mutex>
#include <utility>

#include "backend/brain/clusterer.h"
#include "backend/storage/data_table.h"
#include "backend/storage/database.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/benchmark/hyadapt/configuration.h"

namespace peloton {
namespace storage {

bool ContainsVisibleEntry(std::vector<ItemPointer>& locations,
                          const concurrency::Transaction* transaction);

column_map_type GetStaticColumnMap();

/**
 * Check if the locations contains at least one visible entry to the transaction
 */
bool ContainsVisibleEntry(std::vector<ItemPointer>& locations,
                          const concurrency::Transaction* transaction) {
  auto &manager = catalog::Manager::GetInstance();

  for (auto loc : locations) {

    oid_t tile_group_id = loc.block;
    oid_t tuple_offset = loc.offset;

    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto header = tile_group->GetHeader();

    auto transaction_id = transaction->GetTransactionId();
    auto last_commit_id = transaction->GetLastCommitId();
    bool visible = header->IsVisible(tuple_offset, transaction_id,
                                     last_commit_id);

    if (visible)
      return true;
  }

  return false;
}

DataTable::DataTable(catalog::Schema *schema, AbstractBackend *backend,
                     std::string table_name, oid_t database_oid, oid_t table_oid,
                     size_t tuples_per_tilegroup,
                     bool own_schema,
                     bool adapt_table)
: AbstractTable(database_oid, table_oid, table_name, schema, own_schema),
  backend(backend),
  tuples_per_tilegroup(tuples_per_tilegroup),
  adapt_table(adapt_table){
  // Create a tile group.
  AddDefaultTileGroup();
}

DataTable::~DataTable() {
  // clean up tile groups
  oid_t tile_group_count = GetTileGroupCount();
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
      tile_group_itr++) {
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
// TUPLE HELPER OPERATIONS
//===--------------------------------------------------------------------===//

bool DataTable::CheckNulls(const storage::Tuple *tuple) const {
  assert(schema->GetColumnCount() == tuple->GetColumnCount());

  oid_t column_count = schema->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    if (tuple->IsNull(column_itr) && schema->AllowNull(column_itr) == false) {
      LOG_TRACE(
          "%d th attribute in the tuple was NULL. It is non-nullable " "attribute.",
          column_itr);
      return false;
    }
  }

  return true;
}

bool DataTable::CheckConstraints(const storage::Tuple *tuple) const {
  // First, check NULL constraints
  if (CheckNulls(tuple) == false) {
    throw ConstraintException(
        "Not NULL constraint violated : " + tuple->GetInfo());
    return false;
  }

  return true;
}

ItemPointer DataTable::GetTupleSlot(const concurrency::Transaction *transaction,
                                    const storage::Tuple *tuple) {
  assert(tuple);

  if (CheckConstraints(tuple) == false)
    return INVALID_ITEMPOINTER;

  TileGroup *tile_group = nullptr;
  oid_t tuple_slot = INVALID_OID;
  oid_t tile_group_offset = INVALID_OID;
  auto transaction_id = transaction->GetTransactionId();

  while (tuple_slot == INVALID_OID) {
    // First, figure out last tile group
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      assert(GetTileGroupCount() > 0);
      tile_group_offset = GetTileGroupCount() - 1;
      LOG_TRACE("Tile group offset :: %d \n", tile_group_offset);
    }

    // Then, try to grab a slot in the tile group header
    tile_group = GetTileGroup(tile_group_offset);
    tuple_slot = tile_group->InsertTuple(transaction_id, tuple);
    if (tuple_slot == INVALID_OID) {
      // XXX Should we put this in a critical section?
      AddDefaultTileGroup();
    }
  }

  LOG_INFO("tile group offset: %u, tile group id: %u, address: %p",
           tile_group_offset, tile_group->GetTileGroupId(), tile_group);

  // Set tuple location
  ItemPointer location(tile_group->GetTileGroupId(), tuple_slot);

  return location;
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//

ItemPointer DataTable::InsertTuple(const concurrency::Transaction *transaction,
                                   const storage::Tuple *tuple) {
  // First, do integrity checks and claim a slot
  ItemPointer location = GetTupleSlot(transaction, tuple);
  if (location.block == INVALID_OID){
    LOG_WARN("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_INFO("Location: %d, %d", location.block, location.offset);

  // Index checks and updates
  if (InsertInIndexes(transaction, tuple, location) == false) {
    LOG_WARN("Index constraint violated\n");
    return INVALID_ITEMPOINTER;
  }

  // Increase the table's number of tuples by 1
  IncreaseNumberOfTuplesBy(1);
  // Increase the indexes' number of tuples by 1 as well
  for (auto index : indexes)
    index->IncreaseNumberOfTuplesBy(1);

  return location;
}

/**
 * @brief Insert a tuple into all indexes. If index is primary/unique,
 * check visibility of existing
 * index entries.
 * @warning This still doesn't guarantee serializability.
 *
 * @returns True on success, false if a visible entry exists (in case of primary/unique).
 */
bool DataTable::InsertInIndexes(const concurrency::Transaction *transaction,
                                const storage::Tuple *tuple,
                                ItemPointer location) {

  int index_count = GetIndexCount();

  // (A) Check existence for primary/unique indexes
  // FIXME Since this is NOT protected by a lock, concurrent insert may happen.
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns);

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
      case INDEX_CONSTRAINT_TYPE_UNIQUE: {
        auto locations = index->Scan(key.get());
        auto exist_visible = ContainsVisibleEntry(locations, transaction);
        if (exist_visible) {
          LOG_WARN("A visible index entry exists.");
          return false;
        }
      }
      break;

      case INDEX_CONSTRAINT_TYPE_DEFAULT:
      default:
        break;
    }
    LOG_INFO("Index constraint check on %s passed.", index->GetName().c_str());
  }

  // (B) Insert into index
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns);

    auto status = index->InsertEntry(key.get(), location);
    (void) status;
    assert(status);
  }

  return true;
}

//===--------------------------------------------------------------------===//
// DELETE
//===--------------------------------------------------------------------===//

/**
 * @brief Try to delete a tuple from the table.
 * It may fail because the tuple has been latched or conflict with a future delete.
 *
 * @param transaction_id  The current transaction Id.
 * @param location        ItemPointer of the tuple to delete.
 * NB: location.block should be the tile_group's \b ID, not \b offset.
 * @return True on success, false on failure.
 */
bool DataTable::DeleteTuple(const concurrency::Transaction *transaction,
                            ItemPointer location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto tile_group = GetTileGroupById(tile_group_id);
  txn_id_t transaction_id = transaction->GetTransactionId();
  cid_t last_cid = transaction->GetLastCommitId();

  // Delete slot in underlying tile group
  auto status = tile_group->DeleteTuple(transaction_id, tuple_id, last_cid);
  if (status == false) {
    LOG_WARN("Failed to delete tuple from the tile group : %u , Txn_id : %lu ",
             tile_group_id, transaction_id);
    return false;
  }

  LOG_TRACE("Deleted location :: block = %u offset = %u \n", location.block,
            location.offset);
  // Decrease the table's number of tuples by 1
  DecreaseNumberOfTuplesBy(1);

  return true;
}

//===--------------------------------------------------------------------===//
// UPDATE
//===--------------------------------------------------------------------===//
/**
 * @return Location of the newly inserted (updated) tuple
 */
ItemPointer DataTable::UpdateTuple(const concurrency::Transaction *transaction,
                                   const storage::Tuple *tuple) {
  // Do integrity checks and claim a slot
  ItemPointer location = GetTupleSlot(transaction, tuple);
  if (location.block == INVALID_OID)
    return INVALID_ITEMPOINTER;

  bool status = false;
  // 1) Try as if it's a same-key update
  status = UpdateInIndexes(tuple, location);

  // 2) If 1) fails, try again as an Insert
  if (false == status) {
    status = InsertInIndexes(transaction, tuple, location);
  }

  // 3) If still fails, then it is a real failure
  if (false == status) {
    location = INVALID_ITEMPOINTER;
  }

  return location;
}

bool DataTable::UpdateInIndexes(const storage::Tuple *tuple,
                                const ItemPointer location) {
  for (auto index : indexes) {
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    storage::Tuple *key = new storage::Tuple(index_schema, true);
    key->SetFromTuple(tuple, indexed_columns);

    if (index->UpdateEntry(key, location) == false) {
      LOG_TRACE("Same-key update index failed \n");
      delete key;
      return false;
    }

    delete key;
  }

  return true;
}

//===--------------------------------------------------------------------===//
// STATS
//===--------------------------------------------------------------------===//

/**
 * @brief Increase the number of tuples in this table
 * @param amount amount to increase
 */
void DataTable::IncreaseNumberOfTuplesBy(const float amount) {
  number_of_tuples += amount;
  dirty = true;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void DataTable::DecreaseNumberOfTuplesBy(const float amount) {
  number_of_tuples -= amount;
  dirty = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void DataTable::SetNumberOfTuples(const float num_tuples) {
  number_of_tuples = num_tuples;
  dirty = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
float DataTable::GetNumberOfTuples() const {
  return number_of_tuples;
}

/**
 * @brief return dirty flag
 * @return dirty flag
 */
bool DataTable::IsDirty() const {
  return dirty;
}

/**
 * @brief Reset dirty flag
 */
void DataTable::ResetDirty() {
  dirty = false;
}

//===--------------------------------------------------------------------===//
// TILE GROUP
//===--------------------------------------------------------------------===//

TileGroup *DataTable::GetTileGroupWithLayout(column_map_type partitioning){

  std::vector<catalog::Schema> schemas;
  oid_t tile_group_id = INVALID_OID;

  tile_group_id = catalog::Manager::GetInstance().GetNextOid();

  // Figure out the columns in each tile in new layout
  std::map<std::pair<oid_t, oid_t>, oid_t> tile_column_map;
  for(auto entry : partitioning) {
    tile_column_map[entry.second] = entry.first;
  }

  // Build the schema tile at a time
  std::map<oid_t, std::vector<catalog::Column> > tile_schemas;
  for(auto entry : tile_column_map) {
    tile_schemas[entry.first.first].push_back(schema->GetColumn(entry.second));
  }
  for(auto entry: tile_schemas){
    catalog::Schema tile_schema(entry.second);
    schemas.push_back(tile_schema);
  }

  TileGroup *tile_group = TileGroupFactory::GetTileGroup(
      database_oid, table_oid, tile_group_id, this, backend, schemas,
      partitioning, tuples_per_tilegroup);

  return tile_group;
}

column_map_type DataTable::GetTileGroupLayout(LayoutType layout_type) {
  column_map_type column_map;

  auto col_count = schema->GetColumnCount();
  if(adapt_table == false)
    layout_type = LAYOUT_ROW;

  // pure row layout map
  if(layout_type == LAYOUT_ROW) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(0, col_itr);
    }
  }
  // pure column layout map
  else if(layout_type == LAYOUT_COLUMN) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(col_itr, 0);
    }
  }
  // hybrid layout map
  else if(layout_type == LAYOUT_HYBRID){
    // TODO: Fallback option for regular tables
    if(col_count < 10) {
      for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
        column_map[col_itr] = std::make_pair(0, col_itr);
      }
    }
    else {
      column_map = GetStaticColumnMap();
    }
  }
  else{
    throw Exception("Unknown tilegroup layout option : " + std::to_string(layout_type));
  }

  return column_map;
}

oid_t DataTable::AddDefaultTileGroup() {
  column_map_type column_map;
  oid_t tile_group_id = INVALID_OID;

  // First, figure out the partitioning for given tilegroup layout
  column_map = GetTileGroupLayout(peloton_layout);

  TileGroup *tile_group = GetTileGroupWithLayout(column_map);
  assert(tile_group);
  tile_group_id = tile_group->GetTileGroupId();

  LOG_TRACE("Trying to add a tile group \n");
  {
    std::lock_guard<std::mutex> lock(table_mutex);

    // Check if we actually need to allocate a tile group

    // (A) no tile groups in table
    if (tile_groups.empty()) {
      LOG_TRACE("Added first tile group \n");
      tile_groups.push_back(tile_group->GetTileGroupId());
      // add tile group metadata in locator
      catalog::Manager::GetInstance().SetTileGroup(tile_group_id, tile_group);
      LOG_TRACE("Recording tile group : %d \n", tile_group_id);
      return tile_group_id;
    }

    // (B) no slots in last tile group in table
    auto last_tile_group_offset = GetTileGroupCount() - 1;
    auto last_tile_group = GetTileGroup(last_tile_group_offset);

    oid_t active_tuple_count = last_tile_group->GetNextTupleSlot();
    oid_t allocated_tuple_count = last_tile_group->GetAllocatedTupleCount();
    if (active_tuple_count < allocated_tuple_count) {
      LOG_TRACE("Slot exists in last tile group :: %d %d \n", active_tuple_count,
                allocated_tuple_count);
      delete tile_group;
      return INVALID_OID;
    }

    LOG_TRACE("Added a tile group \n");
    tile_groups.push_back(tile_group->GetTileGroupId());

    // add tile group metadata in locator
    catalog::Manager::GetInstance().SetTileGroup(tile_group_id, tile_group);
    LOG_TRACE("Recording tile group : %d \n", tile_group_id);
  }

  return tile_group_id;
}

oid_t DataTable::AddTileGroupWithOid(oid_t tile_group_id){
  assert(tile_group_id);

  std::vector<catalog::Schema> schemas;
  schemas.push_back(*schema);

  column_map_type column_map;
  // default column map
  auto col_count = schema->GetColumnCount();
  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    column_map[col_itr] = std::make_pair(0, col_itr);
  }

  TileGroup *tile_group = TileGroupFactory::GetTileGroup(
      database_oid, table_oid, tile_group_id, this, backend, schemas,
      column_map, tuples_per_tilegroup);

  LOG_TRACE("Trying to add a tile group \n");
  {
    std::lock_guard<std::mutex> lock(table_mutex);

    LOG_TRACE("Added a tile group \n");
    tile_groups.push_back(tile_group->GetTileGroupId());

    // add tile group metadata in locator
    catalog::Manager::GetInstance().SetTileGroup(tile_group_id, tile_group);
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
    catalog::Manager::GetInstance().SetTileGroup(tile_group_id, tile_group);
    LOG_TRACE("Recording tile group : %d \n", tile_group_id);
  }
}

size_t DataTable::GetTileGroupCount() const {
  size_t size = tile_groups.size();
  return size;
}

TileGroup *DataTable::GetTileGroup(oid_t tile_group_offset) const {
  assert(tile_group_offset < GetTileGroupCount());
  auto tile_group_id = tile_groups[tile_group_offset];
  return GetTileGroupById(tile_group_id);
}

TileGroup *DataTable::GetTileGroupById(oid_t tile_group_id) const {
  auto &manager = catalog::Manager::GetInstance();
  storage::TileGroup *tile_group = manager.GetTileGroup(tile_group_id);
  assert(tile_group);
  return tile_group;
}

std::ostream &operator<<(std::ostream &os, const DataTable &table) {
  os << "=====================================================\n";
  os << "TABLE :\n";

  oid_t tile_group_count = table.GetTileGroupCount();
  os << "Tile Group Count : " << tile_group_count << "\n";

  oid_t tuple_count = 0;
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
      tile_group_itr++) {
    auto tile_group = table.GetTileGroup(tile_group_itr);
    auto tile_tuple_count = tile_group->GetNextTupleSlot();

    os << "Tile Group Id  : " << tile_group_itr << " Tuple Count : "
        << tile_tuple_count << "\n";
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
  if (index_type == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
    has_primary_key = true;
  } else if (index_type == INDEX_CONSTRAINT_TYPE_UNIQUE) {
    unique_constraint_count++;
  }
}

index::Index *DataTable::GetIndexWithOid(const oid_t index_oid) const {
  for (auto index : indexes)
    if (index->GetOid() == index_oid)
      return index;

  return nullptr;
}

void DataTable::DropIndexWithOid(const oid_t index_id) {
  {
    std::lock_guard<std::mutex> lock(table_mutex);

    oid_t index_offset = 0;
    for (auto index : indexes) {
      if (index->GetOid() == index_id)
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
    catalog::Schema *schema = this->GetSchema();
    catalog::Constraint constraint(CONSTRAINT_TYPE_FOREIGN,
                                   key->GetConstraintName());
    constraint.SetForeignKeyListOffset(GetForeignKeyCount());
    for (auto fk_column : key->GetFKColumnNames()) {
      schema->AddConstraint(fk_column, constraint);
    }
    // TODO :: We need this one..
    catalog::ForeignKey *fk = new catalog::ForeignKey(*key);
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

// Get the schema for the new transformed tile group
std::vector<catalog::Schema> TransformTileGroupSchema(
    storage::TileGroup *tile_group, const column_map_type &column_map) {
  std::vector<catalog::Schema> new_schema;
  oid_t orig_tile_offset, orig_tile_column_offset;
  oid_t new_tile_offset, new_tile_column_offset;

  // First, get info from the original tile group's schema
  std::map<oid_t, std::map<oid_t, catalog::Column>> schemas;
  auto orig_schemas = tile_group->GetTileSchemas();
  for (auto column_map_entry : column_map) {
    new_tile_offset = column_map_entry.second.first;
    new_tile_column_offset = column_map_entry.second.second;
    oid_t column_offset = column_map_entry.first;

    tile_group->LocateTileAndColumn(column_offset, orig_tile_offset,
                                    orig_tile_column_offset);

    // Get the column info from original schema
    auto orig_schema = orig_schemas[orig_tile_offset];
    auto column_info = orig_schema.GetColumn(orig_tile_column_offset);
    schemas[new_tile_offset][new_tile_column_offset] = column_info;
  }

  // Then, build the new schema
  for (auto schemas_tile_entry : schemas) {
    std::vector<catalog::Column> columns;
    for (auto schemas_column_entry : schemas_tile_entry.second)
      columns.push_back(schemas_column_entry.second);

    catalog::Schema tile_schema(columns);
    new_schema.push_back(tile_schema);
  }

  return new_schema;
}

// Set the transformed tile group column-at-a-time
void SetTransformedTileGroup(storage::TileGroup *orig_tile_group,
                             storage::TileGroup *new_tile_group) {
  // Check the schema of the two tile groups
  auto new_column_map = new_tile_group->GetColumnMap();
  auto orig_column_map = orig_tile_group->GetColumnMap();
  assert(new_column_map.size() == orig_column_map.size());

  oid_t orig_tile_offset, orig_tile_column_offset;
  oid_t new_tile_offset, new_tile_column_offset;

  auto column_count = new_column_map.size();
  auto tuple_count = orig_tile_group->GetAllocatedTupleCount();
  // Go over each column copying onto the new tile group
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    // Locate the original base tile and tile column offset
    orig_tile_group->LocateTileAndColumn(column_itr, orig_tile_offset,
                                         orig_tile_column_offset);

    new_tile_group->LocateTileAndColumn(column_itr, new_tile_offset,
                                        new_tile_column_offset);

    auto orig_tile = orig_tile_group->GetTile(orig_tile_offset);
    auto new_tile = new_tile_group->GetTile(new_tile_offset);

    // Copy the column over to the new tile group
    for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
      auto val = orig_tile->GetValue(tuple_itr, orig_tile_column_offset);
      new_tile->SetValue(val, tuple_itr, new_tile_column_offset);
    }
  }

  // Finally, copy over the tile header
  auto header = orig_tile_group->GetHeader();
  auto new_header = new_tile_group->GetHeader();
  *new_header = *header;
}

storage::TileGroup *DataTable::TransformTileGroup(
    oid_t tile_group_id, const column_map_type &column_map, bool cleanup) {
  // First, check if the tile group is in this table
  {
    std::lock_guard<std::mutex> lock(table_mutex);

    auto found_itr = std::find(tile_groups.begin(), tile_groups.end(),
                               tile_group_id);

    if (found_itr == tile_groups.end()) {
      LOG_ERROR("Tile group not found in table : %u \n", tile_group_id);
      return nullptr;
    }
  }

  // Get orig tile group from catalog
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto tile_group = catalog_manager.GetTileGroup(tile_group_id);

  // Get the schema for the new transformed tile group
  auto new_schema = TransformTileGroupSchema(tile_group, column_map);

  // Allocate space for the transformed tile group
  auto new_tile_group = TileGroupFactory::GetTileGroup(
      tile_group->GetDatabaseId(), tile_group->GetTableId(),
      tile_group->GetTileGroupId(), tile_group->GetAbstractTable(),
      tile_group->GetBackend(), new_schema, column_map,
      tile_group->GetAllocatedTupleCount());

  // Set the transformed tile group column-at-a-time
  SetTransformedTileGroup(tile_group, new_tile_group);

  // Set the location of the new tile group
  catalog_manager.SetTileGroup(tile_group_id, new_tile_group);

  // Clean up the orig tile group, if needed which is normally the case
  if (cleanup)
    delete tile_group;

  return new_tile_group;
}

void DataTable::RecordSample(const brain::Sample& sample) {

  // Add sample
  {
    std::lock_guard<std::mutex> lock(clustering_mutex);
    samples.push_back(sample);
  }

  if(rand() % 10 < 2)
    UpdateDefaultPartition();

}

void DataTable::UpdateDefaultPartition() {

  oid_t cluster_count = 4;
  oid_t column_count = GetSchema()->GetColumnCount();

  brain::Clusterer clusterer(cluster_count, column_count);

  // Process all samples
  {
    std::lock_guard<std::mutex> lock(clustering_mutex);

    for(auto sample : samples)
      clusterer.ProcessSample(sample);
  }

  auto partitioning = clusterer.GetPartitioning(2);

  std::cout << "UPDATED PARTITIONING \n";
  std::cout << "COLUMN "
      << "\t"
      << " TILE"
      << "\n";
  for (auto entry : partitioning)
    std::cout << entry.first << "\t"
    << entry.second.first << " : " << entry.second.second << "\n";
}

//===--------------------------------------------------------------------===//
// UTILS
//===--------------------------------------------------------------------===//

column_map_type GetStaticColumnMap(){
  column_map_type column_map;

  column_map[0] = std::make_pair(0, 0);
  column_map[198] = std::make_pair(0, 1);
  column_map[206] = std::make_pair(0, 2);
  column_map[169] = std::make_pair(0, 3);
  column_map[119] = std::make_pair(0, 4);
  column_map[9] = std::make_pair(0, 5);
  column_map[220] = std::make_pair(0, 6);
  column_map[2] = std::make_pair(0, 7);
  column_map[230] = std::make_pair(0, 8);
  column_map[212] = std::make_pair(0, 9);
  column_map[164] = std::make_pair(0, 10);
  column_map[111] = std::make_pair(0, 11);
  column_map[136] = std::make_pair(0, 12);
  column_map[106] = std::make_pair(0, 13);
  column_map[8] = std::make_pair(0, 14);
  column_map[112] = std::make_pair(0, 15);
  column_map[4] = std::make_pair(0, 16);
  column_map[234] = std::make_pair(0, 17);
  column_map[147] = std::make_pair(0, 18);
  column_map[35] = std::make_pair(0, 19);
  column_map[114] = std::make_pair(0, 20);
  column_map[89] = std::make_pair(0, 21);
  column_map[127] = std::make_pair(0, 22);
  column_map[144] = std::make_pair(0, 23);
  column_map[71] = std::make_pair(0, 24);
  column_map[186] = std::make_pair(0, 25);

  column_map[34] = std::make_pair(1, 0);
  column_map[145] = std::make_pair(1, 1);
  column_map[124] = std::make_pair(1, 2);
  column_map[146] = std::make_pair(1, 3);
  column_map[7] = std::make_pair(1, 4);
  column_map[40] = std::make_pair(1, 5);
  column_map[227] = std::make_pair(1, 6);
  column_map[59] = std::make_pair(1, 7);
  column_map[190] = std::make_pair(1, 8);
  column_map[249] = std::make_pair(1, 9);
  column_map[157] = std::make_pair(1, 10);
  column_map[38] = std::make_pair(1, 11);
  column_map[64] = std::make_pair(1, 12);
  column_map[134] = std::make_pair(1, 13);
  column_map[167] = std::make_pair(1, 14);
  column_map[63] = std::make_pair(1, 15);
  column_map[178] = std::make_pair(1, 16);
  column_map[156] = std::make_pair(1, 17);
  column_map[94] = std::make_pair(1, 18);
  column_map[84] = std::make_pair(1, 19);
  column_map[187] = std::make_pair(1, 20);
  column_map[153] = std::make_pair(1, 21);
  column_map[158] = std::make_pair(1, 22);
  column_map[42] = std::make_pair(1, 23);
  column_map[236] = std::make_pair(1, 24);

  column_map[83] = std::make_pair(2, 0);
  column_map[182] = std::make_pair(2, 1);
  column_map[107] = std::make_pair(2, 2);
  column_map[76] = std::make_pair(2, 3);
  column_map[58] = std::make_pair(2, 4);
  column_map[102] = std::make_pair(2, 5);
  column_map[96] = std::make_pair(2, 6);
  column_map[31] = std::make_pair(2, 7);
  column_map[244] = std::make_pair(2, 8);
  column_map[54] = std::make_pair(2, 9);
  column_map[37] = std::make_pair(2, 10);
  column_map[228] = std::make_pair(2, 11);
  column_map[24] = std::make_pair(2, 12);
  column_map[120] = std::make_pair(2, 13);
  column_map[92] = std::make_pair(2, 14);
  column_map[233] = std::make_pair(2, 15);
  column_map[170] = std::make_pair(2, 16);
  column_map[209] = std::make_pair(2, 17);
  column_map[93] = std::make_pair(2, 18);
  column_map[12] = std::make_pair(2, 19);
  column_map[47] = std::make_pair(2, 20);
  column_map[200] = std::make_pair(2, 21);
  column_map[248] = std::make_pair(2, 22);
  column_map[171] = std::make_pair(2, 23);
  column_map[22] = std::make_pair(2, 24);

  column_map[166] = std::make_pair(3, 0);
  column_map[213] = std::make_pair(3, 1);
  column_map[101] = std::make_pair(3, 2);
  column_map[97] = std::make_pair(3, 3);
  column_map[29] = std::make_pair(3, 4);
  column_map[237] = std::make_pair(3, 5);
  column_map[149] = std::make_pair(3, 6);
  column_map[49] = std::make_pair(3, 7);
  column_map[142] = std::make_pair(3, 8);
  column_map[181] = std::make_pair(3, 9);
  column_map[196] = std::make_pair(3, 10);
  column_map[75] = std::make_pair(3, 11);
  column_map[188] = std::make_pair(3, 12);
  column_map[208] = std::make_pair(3, 13);
  column_map[218] = std::make_pair(3, 14);
  column_map[183] = std::make_pair(3, 15);
  column_map[250] = std::make_pair(3, 16);
  column_map[151] = std::make_pair(3, 17);
  column_map[189] = std::make_pair(3, 18);
  column_map[60] = std::make_pair(3, 19);
  column_map[226] = std::make_pair(3, 20);
  column_map[214] = std::make_pair(3, 21);
  column_map[174] = std::make_pair(3, 22);
  column_map[128] = std::make_pair(3, 23);
  column_map[239] = std::make_pair(3, 24);

  column_map[27] = std::make_pair(4, 0);
  column_map[235] = std::make_pair(4, 1);
  column_map[217] = std::make_pair(4, 2);
  column_map[98] = std::make_pair(4, 3);
  column_map[143] = std::make_pair(4, 4);
  column_map[165] = std::make_pair(4, 5);
  column_map[160] = std::make_pair(4, 6);
  column_map[109] = std::make_pair(4, 7);
  column_map[65] = std::make_pair(4, 8);
  column_map[23] = std::make_pair(4, 9);
  column_map[74] = std::make_pair(4, 10);
  column_map[207] = std::make_pair(4, 11);
  column_map[115] = std::make_pair(4, 12);
  column_map[69] = std::make_pair(4, 13);
  column_map[108] = std::make_pair(4, 14);
  column_map[30] = std::make_pair(4, 15);
  column_map[201] = std::make_pair(4, 16);
  column_map[221] = std::make_pair(4, 17);
  column_map[202] = std::make_pair(4, 18);
  column_map[20] = std::make_pair(4, 19);
  column_map[225] = std::make_pair(4, 20);
  column_map[105] = std::make_pair(4, 21);
  column_map[91] = std::make_pair(4, 22);
  column_map[95] = std::make_pair(4, 23);
  column_map[150] = std::make_pair(4, 24);

  column_map[123] = std::make_pair(5, 0);
  column_map[16] = std::make_pair(5, 1);
  column_map[238] = std::make_pair(5, 2);
  column_map[81] = std::make_pair(5, 3);
  column_map[3] = std::make_pair(5, 4);
  column_map[219] = std::make_pair(5, 5);
  column_map[204] = std::make_pair(5, 6);
  column_map[68] = std::make_pair(5, 7);
  column_map[203] = std::make_pair(5, 8);
  column_map[73] = std::make_pair(5, 9);
  column_map[41] = std::make_pair(5, 10);
  column_map[66] = std::make_pair(5, 11);
  column_map[192] = std::make_pair(5, 12);
  column_map[113] = std::make_pair(5, 13);
  column_map[216] = std::make_pair(5, 14);
  column_map[117] = std::make_pair(5, 15);
  column_map[99] = std::make_pair(5, 16);
  column_map[126] = std::make_pair(5, 17);
  column_map[53] = std::make_pair(5, 18);
  column_map[1] = std::make_pair(5, 19);
  column_map[139] = std::make_pair(5, 20);
  column_map[116] = std::make_pair(5, 21);
  column_map[229] = std::make_pair(5, 22);
  column_map[100] = std::make_pair(5, 23);
  column_map[215] = std::make_pair(5, 24);

  column_map[48] = std::make_pair(6, 0);
  column_map[10] = std::make_pair(6, 1);
  column_map[86] = std::make_pair(6, 2);
  column_map[211] = std::make_pair(6, 3);
  column_map[17] = std::make_pair(6, 4);
  column_map[224] = std::make_pair(6, 5);
  column_map[122] = std::make_pair(6, 6);
  column_map[51] = std::make_pair(6, 7);
  column_map[103] = std::make_pair(6, 8);
  column_map[85] = std::make_pair(6, 9);
  column_map[110] = std::make_pair(6, 10);
  column_map[50] = std::make_pair(6, 11);
  column_map[162] = std::make_pair(6, 12);
  column_map[129] = std::make_pair(6, 13);
  column_map[243] = std::make_pair(6, 14);
  column_map[67] = std::make_pair(6, 15);
  column_map[133] = std::make_pair(6, 16);
  column_map[138] = std::make_pair(6, 17);
  column_map[193] = std::make_pair(6, 18);
  column_map[141] = std::make_pair(6, 19);
  column_map[232] = std::make_pair(6, 20);
  column_map[118] = std::make_pair(6, 21);
  column_map[159] = std::make_pair(6, 22);
  column_map[199] = std::make_pair(6, 23);
  column_map[39] = std::make_pair(6, 24);

  column_map[154] = std::make_pair(7, 0);
  column_map[137] = std::make_pair(7, 1);
  column_map[163] = std::make_pair(7, 2);
  column_map[179] = std::make_pair(7, 3);
  column_map[77] = std::make_pair(7, 4);
  column_map[194] = std::make_pair(7, 5);
  column_map[130] = std::make_pair(7, 6);
  column_map[46] = std::make_pair(7, 7);
  column_map[32] = std::make_pair(7, 8);
  column_map[125] = std::make_pair(7, 9);
  column_map[241] = std::make_pair(7, 10);
  column_map[246] = std::make_pair(7, 11);
  column_map[140] = std::make_pair(7, 12);
  column_map[26] = std::make_pair(7, 13);
  column_map[78] = std::make_pair(7, 14);
  column_map[177] = std::make_pair(7, 15);
  column_map[148] = std::make_pair(7, 16);
  column_map[223] = std::make_pair(7, 17);
  column_map[185] = std::make_pair(7, 18);
  column_map[197] = std::make_pair(7, 19);
  column_map[61] = std::make_pair(7, 20);
  column_map[195] = std::make_pair(7, 21);
  column_map[18] = std::make_pair(7, 22);
  column_map[80] = std::make_pair(7, 23);
  column_map[231] = std::make_pair(7, 24);

  column_map[222] = std::make_pair(8, 0);
  column_map[70] = std::make_pair(8, 1);
  column_map[191] = std::make_pair(8, 2);
  column_map[52] = std::make_pair(8, 3);
  column_map[72] = std::make_pair(8, 4);
  column_map[155] = std::make_pair(8, 5);
  column_map[88] = std::make_pair(8, 6);
  column_map[175] = std::make_pair(8, 7);
  column_map[43] = std::make_pair(8, 8);
  column_map[172] = std::make_pair(8, 9);
  column_map[173] = std::make_pair(8, 10);
  column_map[13] = std::make_pair(8, 11);
  column_map[152] = std::make_pair(8, 12);
  column_map[180] = std::make_pair(8, 13);
  column_map[62] = std::make_pair(8, 14);
  column_map[121] = std::make_pair(8, 15);
  column_map[25] = std::make_pair(8, 16);
  column_map[55] = std::make_pair(8, 17);
  column_map[247] = std::make_pair(8, 18);
  column_map[36] = std::make_pair(8, 19);
  column_map[15] = std::make_pair(8, 20);
  column_map[210] = std::make_pair(8, 21);
  column_map[56] = std::make_pair(8, 22);
  column_map[6] = std::make_pair(8, 23);
  column_map[104] = std::make_pair(8, 24);

  column_map[14] = std::make_pair(9, 0);
  column_map[132] = std::make_pair(9, 1);
  column_map[135] = std::make_pair(9, 2);
  column_map[168] = std::make_pair(9, 3);
  column_map[176] = std::make_pair(9, 4);
  column_map[28] = std::make_pair(9, 5);
  column_map[245] = std::make_pair(9, 6);
  column_map[11] = std::make_pair(9, 7);
  column_map[184] = std::make_pair(9, 8);
  column_map[131] = std::make_pair(9, 9);
  column_map[161] = std::make_pair(9, 10);
  column_map[5] = std::make_pair(9, 11);
  column_map[21] = std::make_pair(9, 12);
  column_map[242] = std::make_pair(9, 13);
  column_map[87] = std::make_pair(9, 14);
  column_map[44] = std::make_pair(9, 15);
  column_map[45] = std::make_pair(9, 16);
  column_map[205] = std::make_pair(9, 17);
  column_map[57] = std::make_pair(9, 18);
  column_map[19] = std::make_pair(9, 19);
  column_map[33] = std::make_pair(9, 20);
  column_map[90] = std::make_pair(9, 21);
  column_map[240] = std::make_pair(9, 22);
  column_map[79] = std::make_pair(9, 23);
  column_map[82] = std::make_pair(9, 24);

  return std::move(column_map);
}


}  // End storage namespace
}  // End peloton namespace
