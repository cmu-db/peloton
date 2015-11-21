//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// data_table.h
//
// Identification: src/backend/storage/data_table.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/brain/sample.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/catalog/foreign_key.h"
#include "backend/storage/abstract_table.h"
#include "backend/concurrency/transaction.h"

/* Possible values for peloton_tilegroup_layout GUC */
typedef enum LayoutType
{
  LAYOUT_ROW,   /* Pure row layout */
  LAYOUT_COLUMN, /* Pure column layout */
  LAYOUT_HYBRID /* Hybrid layout */
} LayoutType;

/* GUC variables */
extern LayoutType peloton_layout;
extern double     peloton_projectivity;
extern int        peloton_num_groups;
extern bool       peloton_fsm;

extern std::vector<peloton::oid_t> hyadapt_column_ids;

namespace peloton {

typedef std::map<oid_t, std::pair<oid_t, oid_t>> column_map_type;

namespace index{
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
  DataTable(catalog::Schema *schema,
            std::string table_name, oid_t database_oid, oid_t table_oid,
            size_t tuples_per_tilegroup,
            bool own_schema,
            bool adapt_table);

  ~DataTable();

  //===--------------------------------------------------------------------===//
  // TUPLE OPERATIONS
  //===--------------------------------------------------------------------===//

  // insert tuple in table
  ItemPointer InsertTuple(const concurrency::Transaction *transaction,
                          const Tuple *tuple);

  // insert the updated tuple in table
  ItemPointer UpdateTuple(const concurrency::Transaction *transaction,
                          const Tuple *tuple);

  // delete the tuple at given location
  bool DeleteTuple(const concurrency::Transaction *transaction,
                   ItemPointer location);

  //===--------------------------------------------------------------------===//
  // TILE GROUP
  //===--------------------------------------------------------------------===//

  // coerce into adding a new tile group with a tile group id
  oid_t AddTileGroupWithOid(oid_t tile_group_id);

  // add a customized tile group to table
  void AddTileGroup(TileGroup *tile_group);

  // NOTE: This must go through the manager's locator
  // This allows us to "TRANSFORM" tile groups atomically
  // WARNING: We should distinguish OFFSET and ID of a tile group
  TileGroup *GetTileGroup(oid_t tile_group_offset) const;

  TileGroup *GetTileGroupById(oid_t tile_group_id) const;

  size_t GetTileGroupCount() const;

  // Get a tile group with given layout
  TileGroup *GetTileGroupWithLayout(const column_map_type& partitioning);

  //===--------------------------------------------------------------------===//
  // INDEX
  //===--------------------------------------------------------------------===//

  void AddIndex(index::Index *index);

  index::Index *GetIndexWithOid(const oid_t index_oid) const;

  void DropIndexWithOid(const oid_t index_oid);

  index::Index *GetIndex(const oid_t index_offset) const;

  oid_t GetIndexCount() const;

  //===--------------------------------------------------------------------===//
  // FOREIGN KEYS
  //===--------------------------------------------------------------------===//

  void AddForeignKey(catalog::ForeignKey *key);

  catalog::ForeignKey *GetForeignKey(const oid_t key_offset) const;

  void DropForeignKey(const oid_t key_offset);

  oid_t GetForeignKeyCount() const;

  //===--------------------------------------------------------------------===//
  // TRANSFORMERS
  //===--------------------------------------------------------------------===//

  storage::TileGroup *TransformTileGroup(oid_t tile_group_offset,
                                         double theta);

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  void IncreaseNumberOfTuplesBy(const float amount);

  void DecreaseNumberOfTuplesBy(const float amount);

  void SetNumberOfTuples(const float num_tuples);

  float GetNumberOfTuples() const;

  bool IsDirty() const;

  void ResetDirty();

  const column_map_type& GetDefaultPartition();

  //===--------------------------------------------------------------------===//
  // Clustering
  //===--------------------------------------------------------------------===//

  void RecordSample(const brain::Sample& sample);

  void UpdateDefaultPartition();

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  bool HasPrimaryKey() { return has_primary_key; }

  bool HasUniqueConstraints() { return (unique_constraint_count > 0); }

  bool HasForeignKeys() { return (GetForeignKeyCount() > 0); }

  column_map_type GetStaticColumnMap(std::string table_name, oid_t column_count);

  // Get a string representation of this table
  friend std::ostream &operator<<(std::ostream &os, const DataTable &table);

  std::map<oid_t, oid_t> GetColumnMapStats();

 protected:
  //===--------------------------------------------------------------------===//
  // INTEGRITY CHECKS
  //===--------------------------------------------------------------------===//

  bool CheckNulls(const storage::Tuple *tuple) const;

  bool CheckConstraints(const storage::Tuple *tuple) const;

  // Claim a tuple slot in a tile group
  ItemPointer GetTupleSlot(const concurrency::Transaction *transaction,
                           const storage::Tuple *tuple);

  // add a default unpartitioned tile group to table
  oid_t AddDefaultTileGroup();

  // get a partitioning with given layout type
  column_map_type GetTileGroupLayout(LayoutType layout_type);

  //===--------------------------------------------------------------------===//
  // INDEX HELPERS
  //===--------------------------------------------------------------------===//

  // try to insert into the indices
  bool InsertInIndexes(const concurrency::Transaction *transaction,
                       const storage::Tuple *tuple, ItemPointer location);

  /** @return True if it's a same-key update and it's successful */
  bool UpdateInIndexes(const storage::Tuple *tuple,
                       ItemPointer location);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // TODO need some policy ?
  // number of tuples allocated per tilegroup
  size_t tuples_per_tilegroup;

  // set of tile groups
  std::vector<oid_t> tile_groups;

  // INDEXES
  std::vector<index::Index *> indexes;

  // CONSTRAINTS
  std::vector<catalog::ForeignKey *> foreign_keys;

  // table mutex
  std::mutex table_mutex;

  // has a primary key ?
  std::atomic<bool> has_primary_key = ATOMIC_VAR_INIT(false);

  // # of unique constraints
  std::atomic<oid_t> unique_constraint_count = ATOMIC_VAR_INIT(START_OID);

  // # of tuples
  float number_of_tuples = 0.0;

  // dirty flag
  bool dirty = false;

  // clustering mutex
  std::mutex clustering_mutex;

  // adapt table
  bool adapt_table = true;

  // default partition map for table
  column_map_type default_partition;

  // samples for clustering
  std::vector<brain::Sample> samples;
};

}  // End storage namespace
}  // End peloton namespace
