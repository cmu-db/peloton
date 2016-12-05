//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.h
//
// Identification: src/include/storage/data_table.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>

#include "common/platform.h"
#include "container/lock_free_array.h"
#include "index/index.h"
#include "storage/abstract_table.h"
#include "storage/indirection_array.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

extern peloton::LayoutType peloton_layout_mode;

//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//

extern std::vector<peloton::oid_t> sdbench_column_ids;

namespace peloton {

typedef std::map<oid_t, std::pair<oid_t, oid_t>> column_map_type;

namespace brain {
class Sample;
}

namespace catalog {
class ForeignKey;
}

namespace index {
class Index;
}

namespace logging {
class LogManager;
}

namespace concurrency {
class Transaction;
}

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
  friend class logging::LogManager;

  DataTable() = delete;
  DataTable(DataTable const &) = delete;

 public:
  // Table constructor
  DataTable(catalog::Schema *schema, const std::string &table_name,
            const oid_t &database_oid, const oid_t &table_oid,
            const size_t &tuples_per_tilegroup, const bool own_schema,
            const bool adapt_table);

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
                      ItemPointer *index_entry_ptr);

  // insert tuple in table. the pointer to the index entry is returned as
  // index_entry_ptr.
  ItemPointer InsertTuple(const Tuple *tuple,
                          concurrency::Transaction *transaction,
                          ItemPointer **index_entry_ptr = nullptr);
  // designed for tables without primary key. e.g., output table used by
  // aggregate_executor.
  ItemPointer InsertTuple(const Tuple *tuple);

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
  TileGroup *GetTileGroupWithLayout(const column_map_type &partitioning);

  //===--------------------------------------------------------------------===//
  // INDEX
  //===--------------------------------------------------------------------===//

  void AddIndex(std::shared_ptr<index::Index> index);

  // Throw CatalogException if not such index is found
  std::shared_ptr<index::Index> GetIndexWithOid(const oid_t &index_oid);

  void DropIndexWithOid(const oid_t &index_oid);

  std::shared_ptr<index::Index> GetIndex(const oid_t &index_offset);

  std::set<oid_t> GetIndexAttrs(const oid_t &index_offset) const;

  oid_t GetIndexCount() const;

  const std::vector<std::set<oid_t>> &GetIndexColumns() const {
    return indexes_columns_;
  }

  //===--------------------------------------------------------------------===//
  // FOREIGN KEYS
  //===--------------------------------------------------------------------===//

  void AddForeignKey(catalog::ForeignKey *key);

  catalog::ForeignKey *GetForeignKey(const oid_t &key_offset) const;

  void DropForeignKey(const oid_t &key_offset);

  oid_t GetForeignKeyCount() const;

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

  void RecordLayoutSample(const brain::Sample &sample);

  const std::vector<brain::Sample> &GetLayoutSamples() const;

  void ClearLayoutSamples();

  void SetDefaultLayout(const column_map_type &layout);

  //===--------------------------------------------------------------------===//
  // INDEX TUNER
  //===--------------------------------------------------------------------===//

  void RecordIndexSample(const brain::Sample &sample);

  const std::vector<brain::Sample> &GetIndexSamples() const;

  void ClearIndexSamples();

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  bool HasPrimaryKey() { return has_primary_key_; }

  bool HasUniqueConstraints() { return (unique_constraint_count_ > 0); }

  bool HasForeignKeys() { return (GetForeignKeyCount() > 0); }

  std::map<oid_t, oid_t> GetColumnMapStats();

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // try to insert into all indexes.
  // the last argument is the index entry in primary index holding the new
  // tuple.
  bool InsertInIndexes(const storage::Tuple *tuple, ItemPointer location,
                       concurrency::Transaction *transaction,
                       ItemPointer **index_entry_ptr);

  static void SetActiveTileGroupCount(const size_t active_tile_group_count) {
    active_tilegroup_count_ = active_tile_group_count;
  }

  static void SetActiveIndirectionArrayCount(
      const size_t active_indirection_array_count) {
    active_indirection_array_count_ = active_indirection_array_count;
  }

 protected:
  //===--------------------------------------------------------------------===//
  // INTEGRITY CHECKS
  //===--------------------------------------------------------------------===//

  bool CheckNulls(const storage::Tuple *tuple) const;

  bool CheckConstraints(const storage::Tuple *tuple) const;

  // Claim a tuple slot in a tile group
  ItemPointer GetEmptyTupleSlot(const storage::Tuple *tuple);

  // add a tile group to the table
  oid_t AddDefaultTileGroup();
  // add a tile group to the table. replace the active_tile_group_id-th active
  // tile group.
  oid_t AddDefaultTileGroup(const size_t &active_tile_group_id);

  oid_t AddDefaultIndirectionArray(const size_t &active_indirection_array_id);

  // get a partitioning with given layout type
  column_map_type GetTileGroupLayout(LayoutType layout_type);

  // Drop all tile groups of the table. Used by recovery
  void DropTileGroups();

  //===--------------------------------------------------------------------===//
  // INDEX HELPERS
  //===--------------------------------------------------------------------===//

  bool InsertInSecondaryIndexes(const AbstractTuple *tuple,
                                const TargetList *targets_ptr,
                                ItemPointer *index_entry_ptr);

  // check the foreign key constraints
  bool CheckForeignKeyConstraints(const storage::Tuple *tuple);

 public:
  static size_t active_tilegroup_count_;

  static size_t active_indirection_array_count_;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

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
  std::vector<catalog::ForeignKey *> foreign_keys_;

  // has a primary key ?
  std::atomic<bool> has_primary_key_ = ATOMIC_VAR_INIT(false);

  // # of unique constraints
  std::atomic<oid_t> unique_constraint_count_ = ATOMIC_VAR_INIT(START_OID);

  // # of tuples. must be atomic as multiple transactions can perform insert
  // concurrently.
  std::atomic<size_t> number_of_tuples_ = ATOMIC_VAR_INIT(0);

  // dirty flag. for detecting whether the tile group has been used.
  bool dirty_ = false;

  //===--------------------------------------------------------------------===//
  // TUNING MEMBERS
  //===--------------------------------------------------------------------===//

  // adapt table
  bool adapt_table_ = true;

  // default partition map for table
  column_map_type default_partition_;

  // samples for layout tuning
  std::vector<brain::Sample> layout_samples_;

  // layout samples mutex
  std::mutex layout_samples_mutex_;

  // samples for layout tuning
  std::vector<brain::Sample> index_samples_;

  // index samples mutex
  std::mutex index_samples_mutex_;

  static oid_t invalid_tile_group_id;
};

}  // End storage namespace
}  // End peloton namespace
