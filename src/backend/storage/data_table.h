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

#include "backend/bridge/bridge.h"
#include "backend/catalog/foreign_key.h"
#include "backend/storage/abstract_table.h"
#include "backend/storage/backend_vm.h"
#include "backend/index/index.h"

#include <string>

namespace peloton {
namespace storage {

typedef tbb::concurrent_unordered_map<oid_t, index::Index*> oid_t_to_index_ptr;

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
  friend class TableFactory;

  DataTable() = delete;
  DataTable(DataTable const&) = delete;

 public:
  // Table constructor
  DataTable(catalog::Schema *schema,
            AbstractBackend *backend,
            std::string table_name,
            oid_t table_oid,
            size_t tuples_per_tilegroup);

  ~DataTable();

  //===--------------------------------------------------------------------===//
  // OPERATIONS
  //===--------------------------------------------------------------------===//

  std::string GetName() const {
    return table_name;
  }

  oid_t  GetOid() const {
    return table_oid;
  }

  // insert tuple in table
  ItemPointer InsertTuple( txn_id_t transaction_id, const Tuple *tuple, bool update = false );

  void InsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

  bool TryInsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

  void DeleteInIndexes(const storage::Tuple *tuple);

  bool CheckNulls(const storage::Tuple *tuple) const;

  //===--------------------------------------------------------------------===//
  // SCHEMA
  //===--------------------------------------------------------------------===//

  catalog::Schema *GetSchema() {
    return schema;
  }

  //===--------------------------------------------------------------------===//
  // INDEX
  //===--------------------------------------------------------------------===//

  void AddIndex(index::Index *index);

  index::Index* GetIndexWithOid(const oid_t index_oid) const;

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
  // UTILITIES
  //===--------------------------------------------------------------------===//

  bool HasPrimaryKey(){
    return has_primary_key;
  }

  bool HasUniqueConstraints(){
    return (unique_constraint_count > 0);
  }

  bool HasForeignKeys(){
    return (GetForeignKeyCount() > 0);
  }

  // Get a string representation of this table
  friend std::ostream& operator<<(std::ostream& os, const DataTable& table);

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // table name
  std::string table_name;

  // catalog info
  oid_t table_oid;

  // has a primary key ?
  std::atomic<bool> has_primary_key = ATOMIC_VAR_INIT(false);

  // # of unique constraints
  std::atomic<oid_t> unique_constraint_count = ATOMIC_VAR_INIT(START_OID);

  // table mutex
  std::mutex table_mutex;

  // INDEXES

  std::vector<index::Index*> indexes;

  // CONSTRAINTS

  std::vector<catalog::ForeignKey*> foreign_keys;
};


} // End storage namespace
} // End peloton namespace


