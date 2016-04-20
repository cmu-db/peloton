//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.cpp
//
// Identification: src/backend/storage/data_table.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <mutex>
#include <utility>

#include "backend/brain/clusterer.h"
#include "backend/storage/data_table.h"

#include "../benchmark/hyadapt/hyadapt_configuration.h"
#include "backend/storage/database.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"

//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//

std::vector<peloton::oid_t> hyadapt_column_ids;

double peloton_projectivity;

int peloton_num_groups;

bool peloton_fsm;

namespace peloton {
namespace storage {

DataTable::DataTable(catalog::Schema *schema, const std::string &table_name,
                     const oid_t &database_oid, const oid_t &table_oid,
                     const size_t &tuples_per_tilegroup, const bool own_schema,
                     const bool adapt_table)
    : AbstractTable(database_oid, table_oid, table_name, schema, own_schema),
      tuples_per_tilegroup_(tuples_per_tilegroup), 
      adapt_table_(adapt_table) {
  // Init default partition
  auto col_count = schema->GetColumnCount();
  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    default_partition_[col_itr] = std::make_pair(0, col_itr);
  }

  // Create a tile group.
  AddDefaultTileGroup();
}

DataTable::~DataTable() {
  // clean up tile groups by dropping the references in the catalog
  oid_t tile_group_count = GetTileGroupCount();
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    auto tile_group_id = tile_groups_[tile_group_itr];
    catalog::Manager::GetInstance().DropTileGroup(tile_group_id);
  }

  // clean up indices
  for (auto index : indexes_) {
    delete index;
  }

  // clean up foreign keys
  for (auto foreign_key : foreign_keys_) {
    delete foreign_key;
  }

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
          "%lu th attribute in the tuple was NULL. It is non-nullable "
              "attribute.",
          column_itr);
      return false;
    }
  }

  return true;
}

bool DataTable::CheckConstraints(const storage::Tuple *tuple) const {
  // First, check NULL constraints
  if (CheckNulls(tuple) == false) {
    throw ConstraintException("Not NULL constraint violated : " +
        std::string(tuple->GetInfo()));
    return false;
  }
  return true;
}

// this function is called when update/delete/insert is performed.
// this function first checks whether there's available slot.
// if yes, then directly return the available slot.
// in particular, if this is the last slot, a new tile group is created.
// if there's no available slot, then some other threads must be allocating a new tile group.
// we just wait until a new tuple slot in the newly allocated tile group is available.
ItemPointer DataTable::GetEmptyTupleSlot(const storage::Tuple *tuple,
                                    bool check_constraint) {
  assert(tuple);
  if (check_constraint == true && CheckConstraints(tuple) == false) {
    return INVALID_ITEMPOINTER;
  }

  std::shared_ptr<storage::TileGroup> tile_group;
  oid_t tuple_slot = INVALID_OID;
  oid_t tile_group_id = INVALID_OID;

  // get valid tuple.
  while (true) {
    // get the last tile group.
    tile_group = GetTileGroup(tile_group_count_ - 1);

    tuple_slot = tile_group->InsertTuple(tuple);

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      tile_group_id = tile_group->GetTileGroupId();
      break;
    }
  }
  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    AddDefaultTileGroup();
  }

  LOG_TRACE("tile group count: %lu, tile group id: %lu, address: %p",
            tile_group_count_, tile_group->GetTileGroupId(), tile_group.get());

  // Set tuple location
  ItemPointer location(tile_group_id, tuple_slot);

  return location;
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//
ItemPointer DataTable::InsertEmptyVersion(const storage::Tuple *tuple) {
  // First, do integrity checks and claim a slot
  ItemPointer location = GetEmptyTupleSlot(tuple, false);
  if (location.block == INVALID_OID) {
    LOG_WARN("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  // Index checks and updates
  if (InsertInSecondaryIndexes(tuple, location) == false) {
    LOG_WARN("Index constraint violated");
    return INVALID_ITEMPOINTER;
  }

  // ForeignKey checks
  if (CheckForeignKeyConstraints(tuple) == false) {
    LOG_WARN("ForeignKey constraint violated");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %lu, %lu", location.block, location.offset);

  IncreaseNumberOfTuplesBy(1);
  return location;
}

ItemPointer DataTable::InsertVersion(const storage::Tuple *tuple) {
  // First, do integrity checks and claim a slot
  ItemPointer location = GetEmptyTupleSlot(tuple, true);
  if (location.block == INVALID_OID) {
    LOG_WARN("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  // Index checks and updates
  if (InsertInSecondaryIndexes(tuple, location) == false) {
    LOG_WARN("Index constraint violated");
    return INVALID_ITEMPOINTER;
  }

  // ForeignKey checks
  if (CheckForeignKeyConstraints(tuple) == false) {
    LOG_WARN("ForeignKey constraint violated");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %lu, %lu", location.block, location.offset);

  IncreaseNumberOfTuplesBy(1);
  return location;
}

ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple) {
  // First, do integrity checks and claim a slot
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_WARN("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %lu, %lu", location.block, location.offset);

  // Index checks and updates
  if (InsertInIndexes(tuple, location) == false) {
    LOG_WARN("Index constraint violated");
    return INVALID_ITEMPOINTER;
  }

  // ForeignKey checks
  if (CheckForeignKeyConstraints(tuple) == false) {
    LOG_WARN("ForeignKey constraint violated");
    return INVALID_ITEMPOINTER;
  }

  // Increase the table's number of tuples by 1
  IncreaseNumberOfTuplesBy(1);
  // Increase the indexes' number of tuples by 1 as well
  for (auto index : indexes_) index->IncreaseNumberOfTuplesBy(1);

  return location;
}

/**
 * @brief Insert a tuple into all indexes. If index is primary/unique,
 * check visibility of existing
 * index entries.
 * @warning This still doesn't guarantee serializability.
 *
 * @returns True on success, false if a visible entry exists (in case of
 *primary/unique).
 */
// TODO: this function MUST be rewritten!!! --Yingjun
bool DataTable::InsertInIndexes(const storage::Tuple *tuple,
                                ItemPointer location) {
  int index_count = GetIndexCount();
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const storage::Tuple *, const ItemPointer &)> fn
      = std::bind(&concurrency::TransactionManager::IsVisbleOrDirty, &transaction_manager,
                  std::placeholders::_1, std::placeholders::_2);

  // (A) Check existence for primary/unique indexes
  // FIXME Since this is NOT protected by a lock, concurrent insert may happen.
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
      case INDEX_CONSTRAINT_TYPE_UNIQUE: {
        // TODO: get unique tuple from primary index.
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        if (index->ConditionalInsertEntry(key.get(), location, fn) == false) {
          return false;
        }

        // auto locations = index->ScanKey(key.get());
        // auto exist_visible = ContainsVisibleEntry(locations, transaction);
        // if (exist_visible) {
        //   LOG_WARN("A visible index entry exists.");
        //   return false;
        // }
      } break;

      case INDEX_CONSTRAINT_TYPE_DEFAULT:
      default:
        index->InsertEntry(key.get(), location);
        break;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }

  return true;
}

bool DataTable::InsertInSecondaryIndexes(const storage::Tuple *tuple,
                                         ItemPointer location) {
  int index_count = GetIndexCount();
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const storage::Tuple *, const ItemPointer &)> fn
      = std::bind(&concurrency::TransactionManager::IsVisbleOrDirty, &transaction_manager,
                  std::placeholders::_1, std::placeholders::_2);

  // (A) Check existence for primary/unique indexes
  // FIXME Since this is NOT protected by a lock, concurrent insert may happen.
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
        break;
      case INDEX_CONSTRAINT_TYPE_UNIQUE: {
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        if (index->ConditionalInsertEntry(key.get(), location, fn) == false) {
          return false;
        }
        // auto locations = index->ScanKey(key.get());
        // auto exist_visible = ContainsVisibleEntry(locations, transaction);
        // if (exist_visible) {
        //   LOG_WARN("A visible index entry exists.");
        //   return false;
        // }
      } break;

      case INDEX_CONSTRAINT_TYPE_DEFAULT:
      default:
        index->InsertEntry(key.get(), location);
        break;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
  return true;
}

/**
 * @brief Check if all the foreign key constraints on this table
 * is satisfied by checking whether the key exist in the referred table
 *
 * FIXME: this still does not guarantee correctness under concurrent transaction
 *   because it only check if the key exists the referred table's index
 *   -- however this key might be a uncommitted key that is not visible to others
 *   and it might be deleted if that txn abort.
 *   We should modify this function and add logic to check
 *   if the result of the ScanKey is visible.
 *
 * @returns True on success, false if any foreign key constraints fail
 */
bool DataTable::CheckForeignKeyConstraints(const storage::Tuple *tuple) {

  for (auto foreign_key : foreign_keys_) {
    oid_t sink_table_id = foreign_key->GetSinkTableOid();
    storage::DataTable *ref_table =
        (storage::DataTable *)catalog::Manager::GetInstance().GetTableWithOid(
            database_oid, sink_table_id);

    int ref_table_index_count = ref_table->GetIndexCount();

    for (int index_itr = ref_table_index_count - 1; index_itr >= 0; --index_itr) {
      auto index = ref_table->GetIndex(index_itr);

      // The foreign key constraints only refer to the primary key
      if (index->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
        LOG_INFO("BEGIN checking referred table");
        auto key_attrs = foreign_key->GetFKColumnOffsets();

        std::unique_ptr<catalog::Schema> foreign_key_schema(catalog::Schema::CopySchema(schema, key_attrs));
        std::unique_ptr<storage::Tuple> key(new storage::Tuple(foreign_key_schema.get(), true));
        //FIXME: what is the 3rd arg should be?
        key->SetFromTuple(tuple, key_attrs, index->GetPool());

        LOG_INFO("check key: %s", key->GetInfo().c_str());
        auto locations = index->ScanKey(key.get());

        // if this key doesn't exist in the refered column
        if (locations.size() == 0) {
          return false;
        }

        break;
      }
    }
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
void DataTable::IncreaseNumberOfTuplesBy(const float &amount) {
  number_of_tuples_ += amount;
  dirty_ = true;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void DataTable::DecreaseNumberOfTuplesBy(const float &amount) {
  number_of_tuples_ -= amount;
  dirty_ = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void DataTable::SetNumberOfTuples(const float &num_tuples) {
  number_of_tuples_ = num_tuples;
  dirty_ = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
float DataTable::GetNumberOfTuples() const { return number_of_tuples_; }

/**
 * @brief return dirty flag
 * @return dirty flag
 */
bool DataTable::IsDirty() const { return dirty_; }

/**
 * @brief Reset dirty flag
 */
void DataTable::ResetDirty() { dirty_ = false; }

//===--------------------------------------------------------------------===//
// TILE GROUP
//===--------------------------------------------------------------------===//

TileGroup *DataTable::GetTileGroupWithLayout(
    const column_map_type &partitioning) {
  std::vector<catalog::Schema> schemas;
  oid_t tile_group_id = INVALID_OID;

  tile_group_id = catalog::Manager::GetInstance().GetNextOid();

  // Figure out the columns in each tile in new layout
  std::map<std::pair<oid_t, oid_t>, oid_t> tile_column_map;
  for (auto entry : partitioning) {
    tile_column_map[entry.second] = entry.first;
  }

  // Build the schema tile at a time
  std::map<oid_t, std::vector<catalog::Column>> tile_schemas;
  for (auto entry : tile_column_map) {
    tile_schemas[entry.first.first].push_back(schema->GetColumn(entry.second));
  }
  for (auto entry : tile_schemas) {
    catalog::Schema tile_schema(entry.second);
    schemas.push_back(tile_schema);
  }

  TileGroup *tile_group = TileGroupFactory::GetTileGroup(
      database_oid, table_oid, tile_group_id, this, schemas, partitioning,
      tuples_per_tilegroup_);

  return tile_group;
}

column_map_type DataTable::GetTileGroupLayout(LayoutType layout_type) {
  column_map_type column_map;

  auto col_count = schema->GetColumnCount();
  if (adapt_table_ == false) layout_type = LAYOUT_ROW;

  // pure row layout map
  if (layout_type == LAYOUT_ROW) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(0, col_itr);
    }
  }
    // pure column layout map
  else if (layout_type == LAYOUT_COLUMN) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(col_itr, 0);
    }
  }
    // hybrid layout map
  else if (layout_type == LAYOUT_HYBRID) {
    // TODO: Fallback option for regular tables
    if (col_count < 10) {
      for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
        column_map[col_itr] = std::make_pair(0, col_itr);
      }
    } else {
      column_map = GetStaticColumnMap(table_name, col_count);
    }
  } else {
    throw Exception("Unknown tilegroup layout option : " +
        std::to_string(layout_type));
  }

  return column_map;
}

oid_t DataTable::AddDefaultTileGroup() {
  column_map_type column_map;
  oid_t tile_group_id = INVALID_OID;

  // Figure out the partitioning for given tilegroup layout
  column_map = GetTileGroupLayout((LayoutType)peloton_layout_mode);

  // Create a tile group with that partitioning
  std::shared_ptr<TileGroup> tile_group(GetTileGroupWithLayout(column_map));
  assert(tile_group.get());
  tile_group_id = tile_group.get()->GetTileGroupId();

  LOG_TRACE("Trying to add a tile group ");
  {
    LOG_TRACE("Added a tile group ");
    tile_groups_.push_back(tile_group->GetTileGroupId());

    // add tile group metadata in locator
    catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);
    
    // we must guarantee that the compiler always add tile group before adding tile_group_count_.
    COMPILER_MEMORY_FENCE;

    tile_group_count_++;

    LOG_TRACE("Recording tile group : %lu ", tile_group_id);
  }

  return tile_group_id;
}

oid_t DataTable::AddTileGroupWithOid(const oid_t &tile_group_id) {
  assert(tile_group_id);

  std::vector<catalog::Schema> schemas;
  schemas.push_back(*schema);

  column_map_type column_map;
  // default column map
  auto col_count = schema->GetColumnCount();
  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    column_map[col_itr] = std::make_pair(0, col_itr);
  }

  std::shared_ptr<TileGroup> tile_group(TileGroupFactory::GetTileGroup(
      database_oid, table_oid, tile_group_id, this, schemas, column_map,
      tuples_per_tilegroup_));

  
  LOG_TRACE("Added a tile group ");
  tile_groups_.push_back(tile_group->GetTileGroupId());

    // add tile group metadata in locator
  catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

    // we must guarantee that the compiler always add tile group before adding tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %lu ", tile_group_id);

  return tile_group_id;
}

void DataTable::AddTileGroup(const std::shared_ptr<TileGroup> &tile_group) {
  tile_groups_.push_back(tile_group->GetTileGroupId());
  oid_t tile_group_id = tile_group->GetTileGroupId();

    // add tile group in catalog
  catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

    // we must guarantee that the compiler always add tile group before adding tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;


  LOG_TRACE("Recording tile group : %lu ", tile_group_id);
}

size_t DataTable::GetTileGroupCount() const {
  size_t size = tile_groups_.size();
  return size;
}

std::shared_ptr<storage::TileGroup> DataTable::GetTileGroup(
    const oid_t &tile_group_offset) const {
  assert(tile_group_offset < GetTileGroupCount());
  auto tile_group_id = tile_groups_[tile_group_offset];
  return GetTileGroupById(tile_group_id);
}

std::shared_ptr<storage::TileGroup> DataTable::GetTileGroupById(
    const oid_t &tile_group_id) const {
  auto &manager = catalog::Manager::GetInstance();
  return manager.GetTileGroup(tile_group_id);
}

const std::string DataTable::GetInfo() const {
  std::ostringstream os;

  os << "=====================================================\n";
  os << "TABLE :\n";

  oid_t tile_group_count = GetTileGroupCount();
  os << "Tile Group Count : " << tile_group_count << "\n";

  oid_t tuple_count = 0;
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    auto tile_group = GetTileGroup(tile_group_itr);
    auto tile_tuple_count = tile_group->GetNextTupleSlot();

    os << "Tile Group Id  : " << tile_group_itr
        << " Tuple Count : " << tile_tuple_count << "\n";
    os << (*tile_group);

    tuple_count += tile_tuple_count;
  }

  os << "Table Tuple Count :: " << tuple_count << "\n";

  os << "=====================================================\n";

  return os.str();
}

//===--------------------------------------------------------------------===//
// INDEX
//===--------------------------------------------------------------------===//

void DataTable::AddIndex(index::Index *index) {
  {
    std::lock_guard<std::mutex> lock(tile_group_mutex_);
    indexes_.push_back(index);
  }

  // Update index stats
  auto index_type = index->GetIndexType();
  if (index_type == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
    has_primary_key_ = true;
  } else if (index_type == INDEX_CONSTRAINT_TYPE_UNIQUE) {
    unique_constraint_count_++;
  }
}

index::Index *DataTable::GetIndexWithOid(const oid_t &index_oid) const {
  for (auto index : indexes_)
    if (index->GetOid() == index_oid) return index;

  return nullptr;
}

void DataTable::DropIndexWithOid(const oid_t &index_id) {
  {
    std::lock_guard<std::mutex> lock(tile_group_mutex_);

    oid_t index_offset = 0;
    for (auto index : indexes_) {
      if (index->GetOid() == index_id) break;
      index_offset++;
    }
    assert(index_offset < indexes_.size());

    // Drop the index
    indexes_.erase(indexes_.begin() + index_offset);
  }
}

index::Index *DataTable::GetIndex(const oid_t &index_offset) const {
  assert(index_offset < indexes_.size());
  auto index = indexes_.at(index_offset);
  return index;
}

oid_t DataTable::GetIndexCount() const { return indexes_.size(); }

//===--------------------------------------------------------------------===//
// FOREIGN KEYS
//===--------------------------------------------------------------------===//

void DataTable::AddForeignKey(catalog::ForeignKey *key) {
  {
    std::lock_guard<std::mutex> lock(tile_group_mutex_);
    catalog::Schema *schema = this->GetSchema();
    catalog::Constraint constraint(CONSTRAINT_TYPE_FOREIGN,
                                   key->GetConstraintName());
    constraint.SetForeignKeyListOffset(GetForeignKeyCount());
    for (auto fk_column : key->GetFKColumnNames()) {
      schema->AddConstraint(fk_column, constraint);
    }
    // TODO :: We need this one..
    catalog::ForeignKey *fk = new catalog::ForeignKey(*key);
    foreign_keys_.push_back(fk);
  }
}

catalog::ForeignKey *DataTable::GetForeignKey(const oid_t &key_offset) const {
  catalog::ForeignKey *key = nullptr;
  key = foreign_keys_.at(key_offset);
  return key;
}

void DataTable::DropForeignKey(const oid_t &key_offset) {
  {
    std::lock_guard<std::mutex> lock(tile_group_mutex_);
    assert(key_offset < foreign_keys_.size());
    foreign_keys_.erase(foreign_keys_.begin() + key_offset);
  }
}

oid_t DataTable::GetForeignKeyCount() const { return foreign_keys_.size(); }

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
    const oid_t &tile_group_offset, const double &theta) {
  // First, check if the tile group is in this table
  if (tile_group_offset >= tile_groups_.size()) {
    LOG_ERROR("Tile group offset not found in table : %lu ", tile_group_offset);
    return nullptr;
  }

  auto tile_group_id = tile_groups_[tile_group_offset];

  // Get orig tile group from catalog
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto tile_group = catalog_manager.GetTileGroup(tile_group_id);
  auto diff = tile_group->GetSchemaDifference(default_partition_);

  // Check threshold for transformation
  if (diff < theta) {
    return nullptr;
  }

  // Get the schema for the new transformed tile group
  auto new_schema =
      TransformTileGroupSchema(tile_group.get(), default_partition_);

  // Allocate space for the transformed tile group
  std::shared_ptr<storage::TileGroup> new_tile_group(
      TileGroupFactory::GetTileGroup(
          tile_group->GetDatabaseId(), tile_group->GetTableId(),
          tile_group->GetTileGroupId(), tile_group->GetAbstractTable(),
          new_schema, default_partition_, tile_group->GetAllocatedTupleCount()));

  // Set the transformed tile group column-at-a-time
  SetTransformedTileGroup(tile_group.get(), new_tile_group.get());

  // Set the location of the new tile group
  // and clean up the orig tile group
  catalog_manager.AddTileGroup(tile_group_id, new_tile_group);

  return new_tile_group.get();
}

void DataTable::RecordSample(const brain::Sample &sample) {
  // Add sample
  {
    std::lock_guard<std::mutex> lock(clustering_mutex_);
    samples_.push_back(sample);
  }
}

const column_map_type &DataTable::GetDefaultPartition() {
  return default_partition_;
}

std::map<oid_t, oid_t> DataTable::GetColumnMapStats() {
  std::map<oid_t, oid_t> column_map_stats;

  // Cluster per-tile column count
  for (auto entry : default_partition_) {
    auto tile_id = entry.second.first;
    auto column_map_itr = column_map_stats.find(tile_id);
    if (column_map_itr == column_map_stats.end())
      column_map_stats[tile_id] = 1;
    else
      column_map_stats[tile_id]++;
  }

  return std::move(column_map_stats);
}

void DataTable::UpdateDefaultPartition() {
  oid_t column_count = GetSchema()->GetColumnCount();

  // TODO: Number of clusters and new sample weight
  oid_t cluster_count = 4;
  double new_sample_weight = 0.01;

  brain::Clusterer clusterer(cluster_count, column_count, new_sample_weight);

  // Process all samples
  {
    std::lock_guard<std::mutex> lock(clustering_mutex_);

    // Check if we have any samples
    if (samples_.empty()) return;

    for (auto sample : samples_) {
      clusterer.ProcessSample(sample);
    }

    samples_.clear();
  }

  // TODO: Max number of tiles
  default_partition_ = clusterer.GetPartitioning(2);
}

//===--------------------------------------------------------------------===//
// UTILS
//===--------------------------------------------------------------------===//

column_map_type DataTable::GetStaticColumnMap(const std::string &table_name,
                                              const oid_t &column_count) {
  column_map_type column_map;

  // HYADAPT
  if (table_name == "HYADAPTTABLE") {
    // FSM MODE
    if (peloton_fsm == true) {
      for (oid_t column_id = 0; column_id < column_count; column_id++) {
        column_map[column_id] = std::make_pair(0, column_id);
      }
      return std::move(column_map);

      // TODO: ADD A FSM
      // return default_partition;
    }

    // DEFAULT
    if (peloton_num_groups == 0) {
      oid_t split_point = peloton_projectivity * (column_count - 1);
      oid_t rest_column_count = (column_count - 1) - split_point;

      column_map[0] = std::make_pair(0, 0);
      for (oid_t column_id = 0; column_id < split_point; column_id++) {
        auto hyadapt_column_id = hyadapt_column_ids[column_id];
        column_map[hyadapt_column_id] = std::make_pair(0, column_id + 1);
      }

      for (oid_t column_id = 0; column_id < rest_column_count; column_id++) {
        auto hyadapt_column_id = hyadapt_column_ids[split_point + column_id];
        column_map[hyadapt_column_id] = std::make_pair(1, column_id);
      }
    }
      // MULTIPLE GROUPS
    else {
      column_map[0] = std::make_pair(0, 0);
      oid_t tile_column_count = column_count / peloton_num_groups;

      for (oid_t column_id = 1; column_id < column_count; column_id++) {
        auto hyadapt_column_id = hyadapt_column_ids[column_id - 1];
        int tile_id = (column_id - 1) / tile_column_count;
        oid_t tile_column_id;
        if (tile_id == 0)
          tile_column_id = (column_id) % tile_column_count;
        else
          tile_column_id = (column_id - 1) % tile_column_count;

        if (tile_id >= peloton_num_groups) tile_id = peloton_num_groups - 1;

        column_map[hyadapt_column_id] = std::make_pair(tile_id, tile_column_id);
      }
    }

  }
    // YCSB
  else if (table_name == "USERTABLE") {
    column_map[0] = std::make_pair(0, 0);

    for (oid_t column_id = 1; column_id < column_count; column_id++) {
      column_map[column_id] = std::make_pair(1, column_id - 1);
    }
  }
    // FALLBACK
  else {
    for (oid_t column_id = 0; column_id < column_count; column_id++) {
      column_map[column_id] = std::make_pair(0, column_id);
    }
  }

  return std::move(column_map);
}

}  // End storage namespace
}  // End peloton namespace
