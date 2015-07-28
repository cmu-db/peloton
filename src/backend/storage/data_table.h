/*-------------------------------------------------------------------------
 *
 * data_table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/bridge/ddl/bridge.h"
#include "backend/catalog/foreign_key.h"
#include "backend/index/index.h"
#include "backend/storage/abstract_table.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_factory.h"

#include <string>

namespace peloton {
namespace storage {

typedef tbb::concurrent_unordered_map<oid_t, index::Index *> oid_t_to_index_ptr;

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
  DataTable(catalog::Schema *schema, AbstractBackend *backend,
            std::string table_name, oid_t table_oid,
            size_t tuples_per_tilegroup);

  ~DataTable();

  //===--------------------------------------------------------------------===//
  // TUPLE OPERATIONS
  //===--------------------------------------------------------------------===//

  // insert tuple in table
  ItemPointer InsertTuple(txn_id_t transaction_id, const Tuple *tuple,
                          bool update = false);

  void InsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

  bool TryInsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

  bool DeleteTuple(txn_id_t transaction_id, ItemPointer location);

  void DeleteInIndexes(const storage::Tuple *tuple);

  bool CheckNulls(const storage::Tuple *tuple) const;

  //===--------------------------------------------------------------------===//
  // TILE GROUP
  //===--------------------------------------------------------------------===//

  // add a default unpartitioned tile group to table
  oid_t AddDefaultTileGroup();

  // add a customized tile group to table
  void AddTileGroup(TileGroup *tile_group);

  // NOTE: This must go through the manager's locator
  // This allows us to "TRANSFORM" tile groups atomically
  // WARNING: We should distinguish OFFSET and ID of a tile group
  TileGroup *GetTileGroup(oid_t tile_group_offset) const;

  TileGroup *GetTileGroupById(oid_t tile_group_id) const;

  size_t GetTileGroupCount() const;

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
  // STATS
  //===--------------------------------------------------------------------===//

  void IncreaseNumberOfTuplesBy(const float amount);

  void DecreaseNumberOfTuplesBy(const float amount);

  void SetNumberOfTuples(const float num_tuples);

  float GetNumberOfTuples() const;

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  bool HasPrimaryKey() { return has_primary_key; }

  bool HasUniqueConstraints() { return (unique_constraint_count > 0); }

  bool HasForeignKeys() { return (GetForeignKeyCount() > 0); }

  AbstractBackend *GetBackend() const { return backend; }

  // Get a string representation of this table
  friend std::ostream &operator<<(std::ostream &os, const DataTable &table);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // backend
  AbstractBackend *backend;

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
};

}  // End storage namespace
}  // End peloton namespace
