/*-------------------------------------------------------------------------
 *
 * abstract_table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/manager.h"
#include "storage/tile_group.h"
#include "storage/backend_vm.h"

#include "index/index.h"

#include <string>

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Abstract Table
//===--------------------------------------------------------------------===//

/**
 * Base class for tables
 */
class AbstractTable {

public:
    
    virtual ~AbstractTable();
    
protected:
        
    // Table constructor
    AbstractTable(catalog::Schema *schema, Backend *backend);

public:    
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//
    
    catalog::Schema *GetSchema() const{
        return schema;
    }
    
    Backend *GetBackend() const {
        return backend;
    }

    //===--------------------------------------------------------------------===//
    // OPERATIONS
    //===--------------------------------------------------------------------===//

    // add a default unpartitioned tile group to table
    oid_t AddDefaultTileGroup();

    // add a customized tile group to table
    void AddTileGroup(TileGroup *tile_group);

    // add an index to the table
    void AddIndex(index::Index *index);

    // NOTE: This must go through the manager's locator
    // This allows us to "TRANSFORM" tile groups atomically
    TileGroup *GetTileGroup(oid_t tile_group_id) const;

  size_t GetTileGroupCount() const;

  // insert tuple in table
  ItemPointer InsertTuple(txn_id_t transaction_id, const Tuple *tuple, bool update = false);

  //===--------------------------------------------------------------------===//
  // INDEXES
  //===--------------------------------------------------------------------===//

  size_t GetIndexCount() const {
    return indexes.size();
  }

  index::Index *GetIndex(oid_t index_id) const {
    assert(index_id < indexes.size());
    return indexes[index_id];
  }

  void InsertInIndexes(const storage::Tuple *tuple, ItemPointer location);
  bool TryInsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

  void DeleteInIndexes(const storage::Tuple *tuple);

  bool CheckNulls(const storage::Tuple *tuple) const;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation of this table
  friend std::ostream& operator<<(std::ostream& os, const Table& table);

 protected:

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Catalog information
  oid_t database_id;
  oid_t table_id;

  // backend
  Backend *backend;

  // table schema
  catalog::Schema *schema;

  // table name
  std::string table_name;

  // set of tile groups
  std::vector<oid_t> tile_groups;

  // INDEXES
  std::vector<index::Index*> indexes;

  // TODO need some policy ?
  // number of tuples allocated per tilegroup for this table
  size_t tuples_per_tilegroup = 1000;

  std::mutex table_mutex;

};

//===--------------------------------------------------------------------===//
// Table factory
//===--------------------------------------------------------------------===//

class TableFactory {
 public:
  TableFactory();
  virtual ~TableFactory();

  static Table *GetTable(oid_t database_id,
                         catalog::Schema *schema,
                         std::string table_name = "temp") {

    // create a new backend
    Backend* backend = new VMBackend();

    Table *table =  new Table(schema, backend, table_name);

    table->database_id = database_id;

    return table;
  }

};

} // End storage namespace
} // End nstore namespace


