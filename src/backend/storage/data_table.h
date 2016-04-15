//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.h
//
// Identification: src/backend/storage/data_table.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>

#include "backend/brain/sample.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/catalog/foreign_key.h"
#include "backend/storage/abstract_table.h"
#include "backend/concurrency/transaction.h"
#include "backend/common/platform.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

/* Possible values for peloton_tilegroup_layout GUC */
typedef enum LayoutType {
  LAYOUT_ROW,    /* Pure row layout */
  LAYOUT_COLUMN, /* Pure column layout */
  LAYOUT_HYBRID  /* Hybrid layout */
} LayoutType;

extern LayoutType peloton_layout_mode;

//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//

// Projectivity for determining FSM layout
extern double peloton_projectivity;

// # of groups
extern int peloton_num_groups;

// FSM or not ?
extern bool peloton_fsm;

extern std::vector<peloton::oid_t> hyadapt_column_ids;

namespace peloton {

typedef std::map<oid_t, std::pair<oid_t, oid_t>> column_map_type;

namespace index {
class Index;
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
  oid_t AddTileGroupWithOid(const oid_t &tile_group_id);

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

  void AddIndex(index::Index *index);

  index::Index *GetIndexWithOid(const oid_t &index_oid) const;

  void DropIndexWithOid(const oid_t &index_oid);

  index::Index *GetIndex(const oid_t &index_offset) const;

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

  void IncreaseNumberOfTuplesBy(const float &amount);

  void DecreaseNumberOfTuplesBy(const float &amount);

  void SetNumberOfTuples(const float &num_tuples);

  float GetNumberOfTuples() const;

  bool IsDirty() const;

  void ResetDirty();

  const column_map_type &GetDefaultPartition();

  //===--------------------------------------------------------------------===//
  // Clustering
  //===--------------------------------------------------------------------===//

  void RecordSample(const brain::Sample &sample);

  void UpdateDefaultPartition();

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  bool HasPrimaryKey() { return has_primary_key_; }

  bool HasUniqueConstraints() { return (unique_constraint_count_ > 0); }

  bool HasForeignKeys() { return (GetForeignKeyCount() > 0); }

  column_map_type GetStaticColumnMap(const std::string &table_name,
                                     const oid_t &column_count);

  std::map<oid_t, oid_t> GetColumnMapStats();

  // Get a string representation for debugging
  const std::string GetInfo() const;

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

  //===--------------------------------------------------------------------===//
  // INDEX HELPERS
  //===--------------------------------------------------------------------===//

  // try to insert into the indices
  bool InsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

  bool InsertInSecondaryIndexes(const storage::Tuple *tuple, ItemPointer location);

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
  std::vector<oid_t> tile_groups_;
  std::atomic<size_t> tile_group_count_ = ATOMIC_VAR_INIT(0);
  // current tile group
  //size_t tile_group_offset_ = 0;

  // tile group mutex
  std::mutex tile_group_mutex_;
  Spinlock tile_group_lock_;

  // INDEXES
  std::vector<index::Index *> indexes_;

  // CONSTRAINTS
  std::vector<catalog::ForeignKey *> foreign_keys_;


  // has a primary key ?
  std::atomic<bool> has_primary_key_ = ATOMIC_VAR_INIT(false);

  // # of unique constraints
  std::atomic<oid_t> unique_constraint_count_ = ATOMIC_VAR_INIT(START_OID);

  // # of tuples
  float number_of_tuples_ = 0.0;

  // dirty flag
  bool dirty_ = false;

  // clustering mutex
  std::mutex clustering_mutex_;

  // adapt table
  bool adapt_table_ = true;

  // default partition map for table
  column_map_type default_partition_;

  // samples for clustering
  std::vector<brain::Sample> samples_;
};

}  // End storage namespace
}  // End peloton namespace
