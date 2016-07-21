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

#include <memory>
#include <queue>
#include <map>
#include <mutex>
#include <set>

#include "common/platform.h"
#include "storage/abstract_table.h"
#include "container/lock_free_array.h"
#include "index/index.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

extern LayoutType peloton_layout_mode;

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

namespace storage {

class Tuple;
class TileGroup;

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
  // insert version in table
  ItemPointer InsertEmptyVersion(const Tuple *tuple);
  ItemPointer InsertVersion(const Tuple *tuple);
  // insert tuple in table
  ItemPointer InsertTuple(const Tuple *tuple);

  // delete the tuple at given location
  // bool DeleteTuple(const concurrency::Transaction *transaction,
  //                  ItemPointer location);

  //===--------------------------------------------------------------------===//
  // TILE GROUP
  //===--------------------------------------------------------------------===//

  // coerce into adding a new tile group with a tile group id
  void AddTileGroupWithOidForRecovery(const oid_t &tile_group_id);

  // add a tile group to table
  void AddTileGroup(const std::shared_ptr<TileGroup> &tile_group);

  // Offset is a 0-based number local to the table
  std::shared_ptr<storage::TileGroup> GetTileGroup(
      const oid_t &tile_group_offset) const;

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

  std::shared_ptr<index::Index> GetIndexWithOid(const oid_t &index_oid);

  void DropIndexWithOid(const oid_t &index_oid);

  std::shared_ptr<index::Index> GetIndex(const oid_t &index_offset);

  std::set<oid_t> GetIndexAttrs(const oid_t &index_offset) const;

  oid_t GetIndexCount() const;

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

  const std::vector<brain::Sample>& GetLayoutSamples() const;

  void ClearLayoutSamples();

  void SetDefaultLayout(const column_map_type& layout);

  //===--------------------------------------------------------------------===//
  // INDEX TUNER
  //===--------------------------------------------------------------------===//

  void RecordIndexSample(const brain::Sample &sample);

  const std::vector<brain::Sample>& GetIndexSamples() const;

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

  // insert into specific index
  bool InsertInIndex(oid_t index_offset,
                     const storage::Tuple *tuple,
                     ItemPointer location);

  // try to insert into the indices
  bool InsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

  RWLock &GetTileGroupLock() { return tile_group_lock_; }

 protected:
  //===--------------------------------------------------------------------===//
  // INTEGRITY CHECKS
  //===--------------------------------------------------------------------===//

  bool CheckNulls(const storage::Tuple *tuple) const;

  bool CheckConstraints(const storage::Tuple *tuple) const;

  // Claim a tuple slot in a tile group
  ItemPointer GetEmptyTupleSlot(const storage::Tuple *tuple,
                                bool check_constraint = true);

  // add a default unpartitioned tile group to table
  oid_t AddDefaultTileGroup();

  // get a partitioning with given layout type
  column_map_type GetTileGroupLayout(LayoutType layout_type);

  // Drop all tile groups of the table. Used by recovery
  void DropTileGroups();

  //===--------------------------------------------------------------------===//
  // INDEX HELPERS
  //===--------------------------------------------------------------------===//

  bool InsertInSecondaryIndexes(const storage::Tuple *tuple,
                                ItemPointer location);

  // check the foreign key constraints
  bool CheckForeignKeyConstraints(const storage::Tuple *tuple);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // TODO need some policy ?
  // number of tuples allocated per tilegroup
  size_t tuples_per_tilegroup_;

  // TILE GROUPS
  // set of tile groups
  RWLock tile_group_lock_;

  std::vector<oid_t> tile_groups_;

  std::atomic<size_t> tile_group_count_ = ATOMIC_VAR_INIT(0);

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

  // # of tuples
  size_t number_of_tuples_ = 0.0;

  // dirty flag
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

};

}  // End storage namespace
}  // End peloton namespace
