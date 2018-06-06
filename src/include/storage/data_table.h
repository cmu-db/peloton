//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.h
//
// Identification: src/include/storage/data_table.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>

#include "common/container/lock_free_array.h"
#include "common/item_pointer.h"
#include "common/platform.h"
#include "common/container/lock_free_array.h"
#include "index/index.h"
#include "storage/abstract_table.h"
#include "storage/indirection_array.h"
#include "storage/layout.h"
#include "trigger/trigger.h"

//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//

extern std::vector<peloton::oid_t> sdbench_column_ids;

namespace peloton {

namespace tuning {
class Sample;
}  // namespace indextuner

namespace catalog {
class ForeignKey;
class Catalog;
}  // namespace catalog

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace index {
class Index;
}  // namespace index

namespace logging {
class LogManager;
}  // namespace logging

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

namespace storage {

class Tuple;
class TileGroup;
class IndirectionArray;

//===--------------------------------------------------------------------===//
// DataTable
//===--------------------------------------------------------------------===//

/**
 * Represents a group of tile groups logically vertically contiguous.
 *
 * <Tile Group 1>
 * <Tile Group 2>
 * ...
 * <Tile Group n>
 *
 */
class DataTable : public AbstractTable {
  friend class TileGroup;
  friend class TileGroupFactory;
  friend class TableFactory;
  friend class catalog::Catalog;
  friend class logging::LogManager;

  DataTable() = delete;
  DataTable(DataTable const &) = delete;

 public:
  // Table constructor
  DataTable(catalog::Schema *schema, const std::string &table_name,
            const oid_t &database_oid, const oid_t &table_oid,
            const size_t &tuples_per_tilegroup, const bool own_schema,
            const bool adapt_table, const bool is_catalog = false,
            const peloton::LayoutType layout_type = peloton::LayoutType::ROW);

  ~DataTable();

  //===--------------------------------------------------------------------===//
  // TUPLE OPERATIONS
  //===--------------------------------------------------------------------===//
  // insert an empty version in table. designed for delete operation.
  ItemPointer InsertEmptyVersion();

  // these two functions are designed for reducing memory allocation by
  // performing in-place update.
  // in the update executor, we first acquire a version slot from the data
  // table, and then
  // copy the content into the version. after that, we need to check constraints
  // and then install the version
  // into all the corresponding indexes.
  ItemPointer AcquireVersion();

  // install an version in table. designed for update operation.
  // as we implement logical-pointer indexing mechanism, targets_ptr is
  // required.
  bool InstallVersion(const AbstractTuple *tuple, const TargetList *targets_ptr,
                      concurrency::TransactionContext *transaction,
                      ItemPointer *index_entry_ptr);

  // insert tuple in table. the pointer to the index entry is returned as
  // index_entry_ptr.
  ItemPointer InsertTuple(const Tuple *tuple,
                          concurrency::TransactionContext *transaction,
                          ItemPointer **index_entry_ptr = nullptr,
                          bool check_fk = true);
  // designed for tables without primary key. e.g., output table used by
  // aggregate_executor.
  ItemPointer InsertTuple(const Tuple *tuple);

  // Insert tuple with ItemPointer provided explicitly
  bool InsertTuple(const AbstractTuple *tuple, ItemPointer location,
      concurrency::TransactionContext *transaction, ItemPointer **index_entry_ptr,
      bool check_fk = true);

  //===--------------------------------------------------------------------===//
  // TILE GROUP
  //===--------------------------------------------------------------------===//

  // coerce into adding a new tile group with a tile group id
  void AddTileGroupWithOidForRecovery(const oid_t &tile_group_id);

  void AddTileGroup(const std::shared_ptr<TileGroup> &tile_group);

  // Offset is a 0-based number local to the table
  std::shared_ptr<storage::TileGroup> GetTileGroup(
      const std::size_t &tile_group_offset) const;

  // ID is the global identifier in the entire DBMS
  std::shared_ptr<storage::TileGroup> GetTileGroupById(
      const oid_t &tile_group_id) const;

  size_t GetTileGroupCount() const;

  // Get a tile group with given layout
  TileGroup *GetTileGroupWithLayout(std::shared_ptr<const Layout> layout);

  //===--------------------------------------------------------------------===//
  // TRIGGER
  //===--------------------------------------------------------------------===//

  void AddTrigger(trigger::Trigger new_trigger);

  int GetTriggerNumber();

  trigger::Trigger* GetTriggerByIndex(int n);

  trigger::TriggerList* GetTriggerList();

  void UpdateTriggerListFromCatalog(concurrency::TransactionContext *txn);


  //===--------------------------------------------------------------------===//
  // INDEX
  //===--------------------------------------------------------------------===//

  void AddIndex(std::shared_ptr<index::Index> index);

  // Throw CatalogException if not such index is found
  std::shared_ptr<index::Index> GetIndexWithOid(const oid_t &index_oid);

  void DropIndexWithOid(const oid_t &index_oid);

  void DropIndexes();

  std::shared_ptr<index::Index> GetIndex(const oid_t &index_offset);

  std::set<oid_t> GetIndexAttrs(const oid_t &index_offset) const;

  oid_t GetIndexCount() const;

  oid_t GetValidIndexCount() const;

  const std::vector<std::set<oid_t>> &GetIndexColumns() const {
    return indexes_columns_;
  }
  //===--------------------------------------------------------------------===//
  // FOREIGN KEYS
  //===--------------------------------------------------------------------===//

  bool CheckForeignKeySrcAndCascade(storage::Tuple *prev_tuple, 
                                    storage::Tuple *new_tuple,
                                    concurrency::TransactionContext *transaction,
                                    executor::ExecutorContext *context,
                                    bool is_update);

  void AddForeignKey(catalog::ForeignKey *key);

  catalog::ForeignKey *GetForeignKey(const oid_t &key_offset) const;

  void DropForeignKey(const oid_t &key_offset);

  size_t GetForeignKeyCount() const;

  void RegisterForeignKeySource(catalog::ForeignKey *key);

  size_t GetForeignKeySrcCount() const;

  catalog::ForeignKey *GetForeignKeySrc(const size_t) const;

  //===--------------------------------------------------------------------===//
  // TRANSFORMERS
  //===--------------------------------------------------------------------===//

  storage::TileGroup *TransformTileGroup(const oid_t &tile_group_offset,
                                         const double &theta);

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  void IncreaseTupleCount(const size_t &amount);

  void DecreaseTupleCount(const size_t &amount);

  void SetTupleCount(const size_t &num_tuples);

  size_t GetTupleCount() const;

  bool IsDirty() const;

  void ResetDirty();

  //===--------------------------------------------------------------------===//
  // LAYOUT TUNER
  //===--------------------------------------------------------------------===//

  void RecordLayoutSample(const tuning::Sample &sample);

  std::vector<tuning::Sample> GetLayoutSamples();

  void ClearLayoutSamples();

  void SetDefaultLayout(std::shared_ptr<const Layout> new_layout) {
    PELOTON_ASSERT(new_layout->GetColumnCount() == schema->GetColumnCount());
    default_layout_ = new_layout;
  }

  void ResetDefaultLayout(LayoutType type = LayoutType::ROW) {
    PELOTON_ASSERT((type == LayoutType::ROW) || (type == LayoutType::COLUMN));
    default_layout_ = std::shared_ptr<const Layout>(
        new const Layout(schema->GetColumnCount(), type));
  }
  const Layout &GetDefaultLayout() const;

  //===--------------------------------------------------------------------===//
  // INDEX TUNER
  //===--------------------------------------------------------------------===//

  void RecordIndexSample(const tuning::Sample &sample);

  std::vector<tuning::Sample> GetIndexSamples();

  void ClearIndexSamples();

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  // deprecated, use catalog::TableCatalog::GetInstance()->GetTableName()
  inline std::string GetName() const { return (table_name); }

  // deprecated, use catalog::TableCatalog::GetInstance()->GetDatabaseOid()
  inline oid_t GetDatabaseOid() const { return (database_oid); }

  bool HasPrimaryKey() const { return (has_primary_key_); }

  bool HasUniqueConstraints() const { return (unique_constraint_count_ > 0); }

  bool HasForeignKeys() const { return (foreign_keys_.empty() == false); }

  // try to insert into all indexes.
  // the last argument is the index entry in primary index holding the new
  // tuple.
  bool InsertInIndexes(const AbstractTuple *tuple, ItemPointer location,
                       concurrency::TransactionContext *transaction,
                       ItemPointer **index_entry_ptr);

  inline static size_t GetActiveTileGroupCount() {
    return default_active_tilegroup_count_;
  }

  static void SetActiveTileGroupCount(const size_t active_tile_group_count) {
    default_active_tilegroup_count_ = active_tile_group_count;
  }

  inline static size_t GetActiveIndirectionArrayCount() {
    return default_active_indirection_array_count_;
  }

  static void SetActiveIndirectionArrayCount(
      const size_t active_indirection_array_count) {
    default_active_indirection_array_count_ = active_indirection_array_count;
  }

  // Claim a tuple slot in a tile group
  ItemPointer GetEmptyTupleSlot(const storage::Tuple *tuple);

  hash_t Hash() const;

  bool Equals(const storage::DataTable &other) const;
  bool operator==(const DataTable &rhs) const;
  bool operator!=(const DataTable &rhs) const { return !(*this == rhs); }

 protected:
  //===--------------------------------------------------------------------===//
  // INTEGRITY CHECKS
  //===--------------------------------------------------------------------===//

  bool CheckNotNulls(const AbstractTuple *tuple, oid_t column_idx) const;
//  bool MultiCheckNotNulls(const storage::Tuple *tuple,
//                          std::vector<oid_t> cols) const;

  // bool CheckExp(const storage::Tuple *tuple, oid_t column_idx,
  //              std::pair<ExpressionType, type::Value> exp) const;
  // bool CheckUnique(const storage::Tuple *tuple, oid_t column_idx) const;

  // bool CheckExp(const storage::Tuple *tuple, oid_t column_idx) const;

  bool CheckConstraints(const AbstractTuple *tuple) const;

  // add a tile group to the table
  oid_t AddDefaultTileGroup();
  // add a tile group to the table. replace the active_tile_group_id-th active
  // tile group.
  oid_t AddDefaultTileGroup(const size_t &active_tile_group_id);

  oid_t AddDefaultIndirectionArray(const size_t &active_indirection_array_id);

  // Drop all tile groups of the table. Used by recovery
  void DropTileGroups();

  //===--------------------------------------------------------------------===//
  // INDEX HELPERS
  //===--------------------------------------------------------------------===//

  bool InsertInSecondaryIndexes(const AbstractTuple *tuple,
                                const TargetList *targets_ptr,
                                concurrency::TransactionContext *transaction,
                                ItemPointer *index_entry_ptr);

  // check the foreign key constraints
  bool CheckForeignKeyConstraints(const AbstractTuple *tuple,
                                  concurrency::TransactionContext *transaction);

  //===--------------------------------------------------------------------===//
  // LAYOUT HELPERS
  //===--------------------------------------------------------------------===//

  // Set the current_layout_oid_ to the given value if the current value
  // is less than new_layout_oid. Return true on success.
  // To be used for recovery.
  bool SetCurrentLayoutOid(oid_t new_layout_oid);

  // Performs an atomic increment on the current_layout_oid_
  // and returns the incremented value.
  oid_t GetNextLayoutOid() { return ++current_layout_oid_; }

 private:
  //===--------------------------------------------------------------------===//
  // STATIC MEMBERS
  //===--------------------------------------------------------------------===//
  static size_t default_active_tilegroup_count_;
  static size_t default_active_indirection_array_count_;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  size_t active_tilegroup_count_;
  size_t active_indirection_array_count_;

  const oid_t database_oid;

  // deprecated, use catalog::TableCatalog::GetInstance()->GetTableName()
  std::string table_name;

  // number of tuples allocated per tilegroup
  size_t tuples_per_tilegroup_;

  // TILE GROUPS
  LockFreeArray<oid_t> tile_groups_;

  std::vector<std::shared_ptr<storage::TileGroup>> active_tile_groups_;

  std::atomic<size_t> tile_group_count_ = ATOMIC_VAR_INIT(0);

  // INDIRECTIONS
  std::vector<std::shared_ptr<storage::IndirectionArray>>
      active_indirection_arrays_;

  // data table mutex
  std::mutex data_table_mutex_;

  // INDEXES
  LockFreeArray<std::shared_ptr<index::Index>> indexes_;

  // columns present in the indexes
  std::vector<std::set<oid_t>> indexes_columns_;

  // CONSTRAINTS
  // fk constraints for which this table is the source
  std::vector<catalog::ForeignKey *> foreign_keys_;
  // fk constraints for which this table is the sink
  // The complete information is stored so no need to lookup the table
  // everytime there is a constraint check
  std::vector<catalog::ForeignKey *> foreign_key_sources_;

  // has a primary key ?
  std::atomic<bool> has_primary_key_ = ATOMIC_VAR_INIT(false);

  // # of unique constraints
  std::atomic<oid_t> unique_constraint_count_ = ATOMIC_VAR_INIT(START_OID);

  // # of tuples. must be atomic as multiple transactions can perform insert
  // concurrently.
  std::atomic<size_t> number_of_tuples_ = ATOMIC_VAR_INIT(0);

  // dirty flag. for detecting whether the tile group has been used.
  bool dirty_ = false;

  // Last used layout_oid. Used while creating new layouts
  // Initialized to COLUMN_STORE_OID since its the highest predefined value.
  std::atomic<oid_t> current_layout_oid_;

  //===--------------------------------------------------------------------===//
  // TUNING MEMBERS
  //===--------------------------------------------------------------------===//

  // adapt table
  bool adapt_table_ = true;

  // samples for layout tuning
  std::vector<tuning::Sample> layout_samples_;

  // layout samples mutex
  std::mutex layout_samples_mutex_;

  // samples for layout tuning
  std::vector<tuning::Sample> index_samples_;

  // index samples mutex
  std::mutex index_samples_mutex_;

  static oid_t invalid_tile_group_id;

  // trigger list
  std::unique_ptr<trigger::TriggerList> trigger_list_;
};

}  // namespace storage
}  // namespace peloton
