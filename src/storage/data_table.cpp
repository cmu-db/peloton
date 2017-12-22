//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.cpp
//
// Identification: src/storage/data_table.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <mutex>
#include <utility>

#include "brain/clusterer.h"
#include "brain/sample.h"
#include "catalog/foreign_key.h"
#include "catalog/table_catalog.h"
#include "catalog/trigger_catalog.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/platform.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "gc/gc_manager_factory.h"
#include "index/index.h"
#include "logging/log_manager.h"
#include "storage/abstract_table.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"

//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//

std::vector<peloton::oid_t> sdbench_column_ids;

double peloton_projectivity;

int peloton_num_groups;

namespace peloton {
namespace storage {

oid_t DataTable::invalid_tile_group_id = -1;

size_t DataTable::default_active_tilegroup_count_ = 1;
size_t DataTable::default_active_indirection_array_count_ = 1;

DataTable::DataTable(catalog::Schema *schema, const std::string &table_name,
                     const oid_t &database_oid, const oid_t &table_oid,
                     const size_t &tuples_per_tilegroup, const bool own_schema,
                     const bool adapt_table, const bool is_catalog,
                     const peloton::LayoutType layout_type)
    : AbstractTable(table_oid, schema, own_schema, layout_type),
      database_oid(database_oid),
      table_name(table_name),
      tuples_per_tilegroup_(tuples_per_tilegroup),
      adapt_table_(adapt_table),
      trigger_list_(new trigger::TriggerList()) {

  // Init default partition
  auto col_count = schema->GetColumnCount();
  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    default_partition_[col_itr] = std::make_pair(0, col_itr);
  }

  if (is_catalog == true) {
    active_tilegroup_count_ = 1;
    active_indirection_array_count_ = 1;
  } else {
    active_tilegroup_count_ = default_active_tilegroup_count_;
    active_indirection_array_count_ = default_active_indirection_array_count_;
  }

  active_tile_groups_.resize(active_tilegroup_count_);

  active_indirection_arrays_.resize(active_indirection_array_count_);
  // Create tile groups.
  for (size_t i = 0; i < active_tilegroup_count_; ++i) {
    AddDefaultTileGroup(i);
  }

  // Create indirection layers.
  for (size_t i = 0; i < active_indirection_array_count_; ++i) {
    AddDefaultIndirectionArray(i);
  }
}

DataTable::~DataTable() {
  // clean up tile groups by dropping the references in the catalog
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto tile_groups_size = tile_groups_.GetSize();
  std::size_t tile_groups_itr;

  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
       tile_groups_itr++) {
    auto tile_group_id = tile_groups_.Find(tile_groups_itr);

    if (tile_group_id != invalid_tile_group_id) {
      LOG_TRACE("Dropping tile group : %u ", tile_group_id);
      // drop tile group in catalog
      catalog_manager.DropTileGroup(tile_group_id);
    }
  }

  // clean up foreign keys
  for (auto foreign_key : foreign_keys_) {
    delete foreign_key;
  }

  // drop all indirection arrays
  for (auto indirection_array : active_indirection_arrays_) {
    auto oid = indirection_array->GetOid();
    catalog_manager.DropIndirectionArray(oid);
  }
  // AbstractTable cleans up the schema
}

//===--------------------------------------------------------------------===//
// TUPLE HELPER OPERATIONS
//===--------------------------------------------------------------------===//

bool DataTable::CheckNotNulls(const AbstractTuple *tuple,
                              oid_t column_idx) const {
  if (tuple->GetValue(column_idx).IsNull()) {
    LOG_TRACE(
        "%u th attribute in the tuple was NULL. It is non-nullable "
        "attribute.",
        column_idx);
    return false;
  }
  return true;
}

bool DataTable::CheckConstraints(const AbstractTuple *tuple) const {
  // For each column in the table, check to see whether they have
  // any constraints. Then if they do, make sure that the
  // given tuple does not violate them.
  //
  // TODO: PAVLO 2017-07-15
  //       We should create a faster way of check the constraints for each
  //       column. Like maybe can store a list of just columns that
  //       even have constraints defined so that we don't have to
  //       look at each column individually.
  oid_t column_count = schema->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    std::vector<catalog::Constraint> column_cons =
        schema->GetColumn(column_itr).GetConstraints();
    for (auto cons : column_cons) {
      ConstraintType type = cons.GetType();
      switch (type) {
        case ConstraintType::NOTNULL: {
          if (CheckNotNulls(tuple, column_itr) == false) {
            std::string error = StringUtil::Format(
                "%s constraint violated on column '%s' : %s",
                ConstraintTypeToString(type).c_str(),
                schema->GetColumn(column_itr).GetName().c_str(),
                tuple->GetInfo().c_str());
            throw ConstraintException(error);
          }
          break;
        }
        case ConstraintType::CHECK: {
          //          std::pair<ExpressionType, type::Value> exp =
          //          cons.GetCheckExpression();
          //          if (CheckExp(tuple, column_itr, exp) == false) {
          //            LOG_TRACE("CHECK EXPRESSION constraint violated");
          //            throw ConstraintException(
          //                "CHECK EXPRESSION constraint violated : " +
          //                std::string(tuple->GetInfo()));
          //          }
          break;
        }
        case ConstraintType::UNIQUE: {
          break;
        }
        case ConstraintType::DEFAULT: {
          // Should not be handled here
          // Handled in higher hierarchy
          break;
        }
        case ConstraintType::PRIMARY: {
          break;
        }
        case ConstraintType::FOREIGN: {
          break;
        }
        case ConstraintType::EXCLUSION: {
          break;
        }
        default: {
          std::string error =
              StringUtil::Format("ConstraintType '%s' is not supported",
                                 ConstraintTypeToString(type).c_str());
          LOG_TRACE("%s", error.c_str());
          throw ConstraintException(error);
        }
      }  // SWITCH
    }    // FOR (constraints)
  }      // FOR (columns)
  return true;
}

// this function is called when update/delete/insert is performed.
// this function first checks whether there's available slot.
// if yes, then directly return the available slot.
// in particular, if this is the last slot, a new tile group is created.
// if there's no available slot, then some other threads must be allocating a
// new tile group.
// we just wait until a new tuple slot in the newly allocated tile group is
// available.
// when updating a tuple, we will invoke this function with the argument set to
// nullptr.
// this is because we want to minimize data copy overhead by performing
// in-place update at executor level.
// however, when performing insert, we have to copy data immediately,
// and the argument cannot be set to nullptr.
ItemPointer DataTable::GetEmptyTupleSlot(const storage::Tuple *tuple) {
  //=============== garbage collection==================
  // check if there are recycled tuple slots
  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  auto free_item_pointer = gc_manager.ReturnFreeSlot(this->table_oid);
  if (free_item_pointer.IsNull() == false) {
    // when inserting a tuple
    if (tuple != nullptr) {
      auto tile_group =
          catalog::Manager::GetInstance().GetTileGroup(free_item_pointer.block);
      tile_group->CopyTuple(tuple, free_item_pointer.offset);
    }
    return free_item_pointer;
  }
  //====================================================

  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  std::shared_ptr<storage::TileGroup> tile_group;
  oid_t tuple_slot = INVALID_OID;
  oid_t tile_group_id = INVALID_OID;

  // get valid tuple.
  while (true) {
    // get the last tile group.
    tile_group = active_tile_groups_[active_tile_group_id];

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
    AddDefaultTileGroup(active_tile_group_id);
  }

  LOG_TRACE("tile group count: %lu, tile group id: %u, address: %p",
            tile_group_count_.load(), tile_group->GetTileGroupId(),
            tile_group.get());

  // Set tuple location
  ItemPointer location(tile_group_id, tuple_slot);

  return location;
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//
ItemPointer DataTable::InsertEmptyVersion() {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

ItemPointer DataTable::AcquireVersion() {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

bool DataTable::InstallVersion(const AbstractTuple *tuple,
                               const TargetList *targets_ptr,
                               concurrency::TransactionContext *transaction,
                               ItemPointer *index_entry_ptr) {
  if (CheckConstraints(tuple) == false) {
    LOG_TRACE("InsertVersion(): Constraint violated");
    return false;
  }

  // Index checks and updates
  if (InsertInSecondaryIndexes(tuple, targets_ptr, transaction,
                               index_entry_ptr) == false) {
    LOG_TRACE("Index constraint violated");
    return false;
  }
  return true;
}

ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple,
                                   concurrency::TransactionContext *transaction,
                                   ItemPointer **index_entry_ptr) {
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  auto result = InsertTuple(tuple, location, transaction, index_entry_ptr);
  if (result == false) {
    return INVALID_ITEMPOINTER;
  }
  return location;
}

bool DataTable::InsertTuple(const AbstractTuple *tuple,
    ItemPointer location, concurrency::TransactionContext *transaction,
    ItemPointer **index_entry_ptr) {
  if (CheckConstraints(tuple) == false) {
    LOG_TRACE("InsertTuple(): Constraint violated");
    return false;
  }

  // the upper layer may not pass a index_entry_ptr (default value: nullptr)
  // into the function.
  // in this case, we have to create a temp_ptr to hold the content.
  ItemPointer *temp_ptr = nullptr;
  if (index_entry_ptr == nullptr) {
    index_entry_ptr = &temp_ptr;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  auto index_count = GetIndexCount();
  if (index_count == 0) {
    IncreaseTupleCount(1);
    return true;
  }
  // Index checks and updates
  if (InsertInIndexes(tuple, location, transaction, index_entry_ptr) == false) {
    LOG_TRACE("Index constraint violated");
    return false;
  }

  // ForeignKey checks
  if (CheckForeignKeyConstraints(tuple) == false) {
    LOG_TRACE("ForeignKey constraint violated");
    return false;
  }

  PL_ASSERT((*index_entry_ptr)->block == location.block &&
            (*index_entry_ptr)->offset == location.offset);

  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
  return true;
}

// insert tuple into a table that is without index.
ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple) {
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  UNUSED_ATTRIBUTE auto index_count = GetIndexCount();
  PL_ASSERT(index_count == 0);
  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
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
bool DataTable::InsertInIndexes(const AbstractTuple *tuple,
                                ItemPointer location,
                                concurrency::TransactionContext *transaction,
                                ItemPointer **index_entry_ptr) {
  int index_count = GetIndexCount();

  size_t active_indirection_array_id =
      number_of_tuples_ % active_indirection_array_count_;

  size_t indirection_offset = INVALID_INDIRECTION_OFFSET;

  while (true) {
    auto active_indirection_array =
        active_indirection_arrays_[active_indirection_array_id];
    indirection_offset = active_indirection_array->AllocateIndirection();

    if (indirection_offset != INVALID_INDIRECTION_OFFSET) {
      *index_entry_ptr =
          active_indirection_array->GetIndirectionByOffset(indirection_offset);
      break;
    }
  }

  (*index_entry_ptr)->block = location.block;
  (*index_entry_ptr)->offset = location.offset;

  if (indirection_offset == INDIRECTION_ARRAY_MAX_SIZE - 1) {
    AddDefaultIndirectionArray(active_indirection_array_id);
  }

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                &transaction_manager, transaction, std::placeholders::_1);

  // Since this is NOT protected by a lock, concurrent insert may happen.
  bool res = true;
  int success_count = 0;

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case IndexConstraintType::PRIMARY_KEY:
      case IndexConstraintType::UNIQUE: {
        // get unique tuple from primary/unique index.
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        res = index->CondInsertEntry(key.get(), *index_entry_ptr, fn);
      } break;

      case IndexConstraintType::DEFAULT:
      default:
        index->InsertEntry(key.get(), *index_entry_ptr);
        break;
    }

    // Handle failure
    if (res == false) {
      // If some of the indexes have been inserted,
      // the pointer has a chance to be dereferenced by readers and it cannot be
      // deleted
      *index_entry_ptr = nullptr;
      return false;
    } else {
      success_count += 1;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }

  return true;
}

bool DataTable::InsertInSecondaryIndexes(const AbstractTuple *tuple,
                                         const TargetList *targets_ptr,
                                         concurrency::TransactionContext *transaction,
                                         ItemPointer *index_entry_ptr) {
  int index_count = GetIndexCount();
  // Transform the target list into a hash set
  // when attempting to perform insertion to a secondary index,
  // we must check whether the updated column is a secondary index column.
  // insertion happens only if the updated column is a secondary index column.
  std::unordered_set<oid_t> targets_set;
  for (auto target : *targets_ptr) {
    targets_set.insert(target.first);
  }

  bool res = true;

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                &transaction_manager, transaction, std::placeholders::_1);

  // Check existence for primary/unique indexes
  // Since this is NOT protected by a lock, concurrent insert may happen.
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    if (index->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
      continue;
    }

    // Check if we need to update the secondary index
    bool updated = false;
    for (auto col : indexed_columns) {
      if (targets_set.find(col) != targets_set.end()) {
        updated = true;
        break;
      }
    }

    // If attributes on key are not updated, skip the index update
    if (updated == false) {
      continue;
    }

    // Key attributes are updated, insert a new entry in all secondary index
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));

    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case IndexConstraintType::PRIMARY_KEY:
      case IndexConstraintType::UNIQUE: {
        res = index->CondInsertEntry(key.get(), index_entry_ptr, fn);
      } break;
      case IndexConstraintType::DEFAULT:
      default:
        index->InsertEntry(key.get(), index_entry_ptr);
        break;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
  return res;
}

/**
 * @brief Check if all the foreign key constraints on this table
 * is satisfied by checking whether the key exist in the referred table
 *
 * FIXME: this still does not guarantee correctness under concurrent transaction
 *   because it only check if the key exists the referred table's index
 *   -- however this key might be a uncommitted key that is not visible to
 *   and it might be deleted if that txn abort.
 *   We should modify this function and add logic to check
 *   if the result of the ScanKey is visible.
 *
 * @returns True on success, false if any foreign key constraints fail
 */
bool DataTable::CheckForeignKeyConstraints(
    const AbstractTuple *tuple UNUSED_ATTRIBUTE) {
  for (auto foreign_key : foreign_keys_) {
    oid_t sink_table_id = foreign_key->GetSinkTableOid();
    storage::DataTable *ref_table = nullptr;
    try {
      ref_table = (storage::DataTable *)storage::StorageManager::GetInstance()
                      ->GetTableWithOid(database_oid, sink_table_id);
    } catch (CatalogException &e) {
      LOG_TRACE("Can't find table %d! Return false", sink_table_id);
      return false;
    }
    int ref_table_index_count = ref_table->GetIndexCount();

    for (int index_itr = ref_table_index_count - 1; index_itr >= 0;
         --index_itr) {
      auto index = ref_table->GetIndex(index_itr);
      if (index == nullptr) continue;

      // The foreign key constraints only refer to the primary key
      if (index->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
        LOG_INFO("BEGIN checking referred table");

        std::vector<oid_t> key_attrs = foreign_key->GetSourceColumnIds();
        std::unique_ptr<catalog::Schema> foreign_key_schema(
            catalog::Schema::CopySchema(schema, key_attrs));
        std::unique_ptr<storage::Tuple> key(
            new storage::Tuple(foreign_key_schema.get(), true));
        // FIXME: what is the 3rd arg should be?
        key->SetFromTuple(tuple, key_attrs, index->GetPool());

        LOG_TRACE("check key: %s", key->GetInfo().c_str());

        std::vector<ItemPointer *> location_ptrs;
        index->ScanKey(key.get(), location_ptrs);

        // if this key doesn't exist in the refered column
        if (location_ptrs.size() == 0) {
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
void DataTable::IncreaseTupleCount(const size_t &amount) {
  number_of_tuples_ += amount;
  dirty_ = true;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void DataTable::DecreaseTupleCount(const size_t &amount) {
  number_of_tuples_ -= amount;
  dirty_ = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void DataTable::SetTupleCount(const size_t &num_tuples) {
  number_of_tuples_ = num_tuples;
  dirty_ = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
size_t DataTable::GetTupleCount() const { return number_of_tuples_; }

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
  oid_t tile_group_id = catalog::Manager::GetInstance().GetNextTileGroupId();
  return (AbstractTable::GetTileGroupWithLayout(
      database_oid, tile_group_id, partitioning, tuples_per_tilegroup_));
}

oid_t DataTable::AddDefaultIndirectionArray(
    const size_t &active_indirection_array_id) {
  auto &manager = catalog::Manager::GetInstance();
  oid_t indirection_array_id = manager.GetNextIndirectionArrayId();

  std::shared_ptr<IndirectionArray> indirection_array(
      new IndirectionArray(indirection_array_id));
  manager.AddIndirectionArray(indirection_array_id, indirection_array);

  COMPILER_MEMORY_FENCE;

  active_indirection_arrays_[active_indirection_array_id] = indirection_array;

  return indirection_array_id;
}

oid_t DataTable::AddDefaultTileGroup() {
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  return AddDefaultTileGroup(active_tile_group_id);
}

oid_t DataTable::AddDefaultTileGroup(const size_t &active_tile_group_id) {
  column_map_type column_map;
  oid_t tile_group_id = INVALID_OID;

  // Figure out the partitioning for given tilegroup layout
  column_map = GetTileGroupLayout();

  // Create a tile group with that partitioning
  std::shared_ptr<TileGroup> tile_group(GetTileGroupWithLayout(column_map));
  PL_ASSERT(tile_group.get());

  tile_group_id = tile_group->GetTileGroupId();

  LOG_TRACE("Added a tile group ");
  tile_groups_.Append(tile_group_id);

  // add tile group metadata in locator
  catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

  COMPILER_MEMORY_FENCE;

  active_tile_groups_[active_tile_group_id] = tile_group;

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);

  return tile_group_id;
}

void DataTable::AddTileGroupWithOidForRecovery(const oid_t &tile_group_id) {
  PL_ASSERT(tile_group_id);

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

  auto tile_groups_exists = tile_groups_.Contains(tile_group_id);

  if (tile_groups_exists == false) {
    tile_groups_.Append(tile_group_id);

    LOG_TRACE("Added a tile group ");

    // add tile group metadata in locator
    catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

    // we must guarantee that the compiler always add tile group before adding
    // tile_group_count_.
    COMPILER_MEMORY_FENCE;

    tile_group_count_++;

    LOG_TRACE("Recording tile group : %u ", tile_group_id);
  }
}

// NOTE: This function is only used in test cases.
void DataTable::AddTileGroup(const std::shared_ptr<TileGroup> &tile_group) {
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;

  active_tile_groups_[active_tile_group_id] = tile_group;

  oid_t tile_group_id = tile_group->GetTileGroupId();

  tile_groups_.Append(tile_group_id);

  // add tile group in catalog
  catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);
}

size_t DataTable::GetTileGroupCount() const { return tile_group_count_; }

std::shared_ptr<storage::TileGroup> DataTable::GetTileGroup(
    const std::size_t &tile_group_offset) const {
  PL_ASSERT(tile_group_offset < GetTileGroupCount());

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);

  return GetTileGroupById(tile_group_id);
}

std::shared_ptr<storage::TileGroup> DataTable::GetTileGroupById(
    const oid_t &tile_group_id) const {
  auto &manager = catalog::Manager::GetInstance();
  return manager.GetTileGroup(tile_group_id);
}

void DataTable::DropTileGroups() {
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto tile_groups_size = tile_groups_.GetSize();
  std::size_t tile_groups_itr;

  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
       tile_groups_itr++) {
    auto tile_group_id = tile_groups_.Find(tile_groups_itr);

    if (tile_group_id != invalid_tile_group_id) {
      // drop tile group in catalog
      catalog_manager.DropTileGroup(tile_group_id);
    }
  }

  // Clear array
  tile_groups_.Clear(invalid_tile_group_id);

  tile_group_count_ = 0;
}

//===--------------------------------------------------------------------===//
// INDEX
//===--------------------------------------------------------------------===//

void DataTable::AddIndex(std::shared_ptr<index::Index> index) {
  // Add index
  indexes_.Append(index);

  // Add index column info
  auto index_columns_ = index->GetMetadata()->GetKeyAttrs();
  std::set<oid_t> index_columns_set(index_columns_.begin(),
                                    index_columns_.end());

  indexes_columns_.push_back(index_columns_set);

  // Update index stats
  auto index_type = index->GetIndexType();
  if (index_type == IndexConstraintType::PRIMARY_KEY) {
    has_primary_key_ = true;
  } else if (index_type == IndexConstraintType::UNIQUE) {
    unique_constraint_count_++;
  }
}

std::shared_ptr<index::Index> DataTable::GetIndexWithOid(
    const oid_t &index_oid) {
  std::shared_ptr<index::Index> ret_index;
  auto index_count = indexes_.GetSize();

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    ret_index = indexes_.Find(index_itr);
    if (ret_index != nullptr && ret_index->GetOid() == index_oid) {
      break;
    }
  }
  if (ret_index == nullptr) {
    throw CatalogException("No index with oid = " + std::to_string(index_oid) +
                           " is found");
  }
  return ret_index;
}

void DataTable::DropIndexWithOid(const oid_t &index_oid) {
  oid_t index_offset = 0;
  std::shared_ptr<index::Index> index;
  auto index_count = indexes_.GetSize();

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    index = indexes_.Find(index_itr);
    if (index != nullptr && index->GetOid() == index_oid) {
      break;
    }
  }

  PL_ASSERT(index_offset < indexes_.GetSize());

  // Drop the index
  indexes_.Update(index_offset, nullptr);

  // Drop index column info
  indexes_columns_[index_offset].clear();
}

void DataTable::DropIndexes() {
  // TODO: iterate over all indexes, and actually drop them

  indexes_.Clear(nullptr);

  indexes_columns_.clear();
}

// This is a dangerous function, use GetIndexWithOid() instead. Note
// that the returned index could be a nullptr once we can drop index
// with oid (due to a limitation of LockFreeArray).
std::shared_ptr<index::Index> DataTable::GetIndex(const oid_t &index_offset) {
  PL_ASSERT(index_offset < indexes_.GetSize());
  auto ret_index = indexes_.Find(index_offset);

  return ret_index;
}

//
std::set<oid_t> DataTable::GetIndexAttrs(const oid_t &index_offset) const {
  PL_ASSERT(index_offset < GetIndexCount());

  auto index_attrs = indexes_columns_.at(index_offset);

  return index_attrs;
}

oid_t DataTable::GetIndexCount() const {
  size_t index_count = indexes_.GetSize();

  return index_count;
}

oid_t DataTable::GetValidIndexCount() const {
  std::shared_ptr<index::Index> index;
  auto index_count = indexes_.GetSize();
  oid_t valid_index_count = 0;

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    index = indexes_.Find(index_itr);
    if (index == nullptr) {
      continue;
    }

    valid_index_count++;
  }

  return valid_index_count;
}

//===--------------------------------------------------------------------===//
// FOREIGN KEYS
//===--------------------------------------------------------------------===//

void DataTable::AddForeignKey(catalog::ForeignKey *key) {
  {
    std::lock_guard<std::mutex> lock(data_table_mutex_);
    catalog::Constraint constraint(ConstraintType::FOREIGN,
                                   key->GetConstraintName());
    constraint.SetForeignKeyListOffset(GetForeignKeyCount());
    for (auto fk_column : key->GetSourceColumnIds()) {
      schema->AddConstraint(fk_column, constraint);
    }
    foreign_keys_.push_back(key);
  }
}

catalog::ForeignKey *DataTable::GetForeignKey(const oid_t &key_offset) const {
  catalog::ForeignKey *key = nullptr;
  key = foreign_keys_.at(key_offset);
  return key;
}

void DataTable::DropForeignKey(const oid_t &key_offset) {
  {
    std::lock_guard<std::mutex> lock(data_table_mutex_);
    PL_ASSERT(key_offset < foreign_keys_.size());
    foreign_keys_.erase(foreign_keys_.begin() + key_offset);
  }
}

size_t DataTable::GetForeignKeyCount() const { return foreign_keys_.size(); }

// Adds to the list of tables for which this table's PK is the foreign key sink
void DataTable::RegisterForeignKeySource(const std::string &source_table_name) {
  {
    std::lock_guard<std::mutex> lock(data_table_mutex_);
    foreign_key_sources_.push_back(source_table_name);
  }
}

// Remove a table for which this table's PK is the foreign key sink
void DataTable::RemoveForeignKeySource(const std::string &source_table_name) {
  {
    std::lock_guard<std::mutex> lock(data_table_mutex_);
    for (size_t i = 0; i < foreign_key_sources_.size(); i++) {
      if (foreign_key_sources_[i] == source_table_name) {
        foreign_key_sources_.erase(foreign_key_sources_.begin() + i);
      }
    }
  }
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
  PL_ASSERT(new_column_map.size() == orig_column_map.size());

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
      type::Value val =
          (orig_tile->GetValue(tuple_itr, orig_tile_column_offset));
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
  if (tile_group_offset >= tile_groups_.GetSize()) {
    LOG_ERROR("Tile group offset not found in table : %u ", tile_group_offset);
    return nullptr;
  }

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);

  // Get orig tile group from catalog
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto tile_group = catalog_manager.GetTileGroup(tile_group_id);
  auto diff = tile_group->GetSchemaDifference(default_partition_);

  // Check threshold for transformation
  if (diff < theta) {
    return nullptr;
  }

  LOG_TRACE("Transforming tile group : %u", tile_group_offset);

  // Get the schema for the new transformed tile group
  auto new_schema =
      TransformTileGroupSchema(tile_group.get(), default_partition_);

  // Allocate space for the transformed tile group
  std::shared_ptr<storage::TileGroup> new_tile_group(
      TileGroupFactory::GetTileGroup(
          tile_group->GetDatabaseId(), tile_group->GetTableId(),
          tile_group->GetTileGroupId(), tile_group->GetAbstractTable(),
          new_schema, default_partition_,
          tile_group->GetAllocatedTupleCount()));

  // Set the transformed tile group column-at-a-time
  SetTransformedTileGroup(tile_group.get(), new_tile_group.get());

  // Set the location of the new tile group
  // and clean up the orig tile group
  catalog_manager.AddTileGroup(tile_group_id, new_tile_group);

  return new_tile_group.get();
}

void DataTable::RecordLayoutSample(const brain::Sample &sample) {
  // Add layout sample
  {
    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
    layout_samples_.push_back(sample);
  }
}

std::vector<brain::Sample> DataTable::GetLayoutSamples() {
  {
    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
    return layout_samples_;
  }
}

void DataTable::ClearLayoutSamples() {
  // Clear layout samples list
  {
    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
    layout_samples_.clear();
  }
}

void DataTable::RecordIndexSample(const brain::Sample &sample) {
  // Add index sample
  {
    std::lock_guard<std::mutex> lock(index_samples_mutex_);
    index_samples_.push_back(sample);
  }
}

std::vector<brain::Sample> DataTable::GetIndexSamples() {
  {
    std::lock_guard<std::mutex> lock(index_samples_mutex_);
    return index_samples_;
  }
}

void DataTable::ClearIndexSamples() {
  // Clear index samples list
  {
    std::lock_guard<std::mutex> lock(index_samples_mutex_);
    index_samples_.clear();
  }
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

  return column_map_stats;
}

void DataTable::SetDefaultLayout(const column_map_type &layout) {
  default_partition_ = layout;
}

column_map_type DataTable::GetDefaultLayout() const {
  return default_partition_;
}

void DataTable::AddTrigger(trigger::Trigger new_trigger) {
  trigger_list_->AddTrigger(new_trigger);
}

int DataTable::GetTriggerNumber() {
  return trigger_list_->GetTriggerListSize();
}

trigger::Trigger *DataTable::GetTriggerByIndex(int n) {
  if (trigger_list_->GetTriggerListSize() <= n) return nullptr;
  return trigger_list_->Get(n);
}

trigger::TriggerList *DataTable::GetTriggerList() {
  if (trigger_list_->GetTriggerListSize() <= 0) return nullptr;
  return trigger_list_.get();
}

void DataTable::UpdateTriggerListFromCatalog(concurrency::TransactionContext *txn) {
  trigger_list_ =
      catalog::TriggerCatalog::GetInstance().GetTriggers(table_oid, txn);
}

hash_t DataTable::Hash() const {
  auto oid = GetOid();
  hash_t hash = HashUtil::Hash(&oid);
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(GetName().c_str(),
                                                           GetName().length()));
  auto db_oid = GetOid();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&db_oid));
  return hash;
}

bool DataTable::Equals(const storage::DataTable &other) const {
  return (*this == other);
}

bool DataTable::operator==(const DataTable &rhs) const {
  if (GetName() != rhs.GetName())
    return false;
  if (GetDatabaseOid() != rhs.GetDatabaseOid())
    return false;
  if (GetOid() != rhs.GetOid())
    return false;
  return true;
}

}  // End storage namespace
}  // End peloton namespace
