/*-------------------------------------------------------------------------
 *
 * table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/table.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/manager.h"
#include "storage/tile_group.h"

#include <string>

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Table
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
class Table {
  friend class TileGroup;
  friend class TableFactory;

  Table() = delete;
  Table(Table const&) = delete;

 public:

  // Table constructor
  Table(catalog::Manager *catalog,
        catalog::Schema *schema,
        Backend *backend,
        id_t tuple_count,
        bool own_backend,
        bool own_schema)
 : catalog(catalog),
   schema(schema),
   backend(backend),
   own_backend(own_backend),
   own_schema(own_schema),
   tuple_count(tuple_count),
   table_id(INVALID_ID),
   database_id(INVALID_ID) {
  }

  ~Table() {
    // clean up tile groups
    for(auto tile_group : tile_groups)
      delete tile_group;

    if(own_schema)
      delete schema;

    if(own_backend)
      delete backend;

  }

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  // add a new tile group to table
  id_t AddTileGroup();

 protected:

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // set of tile groups
  std::vector<TileGroup*> tile_groups;

  // manager
  catalog::Manager *catalog;

  // table schema
  catalog::Schema* schema;

  // backend
  Backend *backend;

  bool own_backend;
  bool own_schema;

  id_t tuple_count;

  // Catalog information
  id_t table_id;
  id_t database_id;

  std::mutex table_mutex;
};

//===--------------------------------------------------------------------===//
// Table factory
//===--------------------------------------------------------------------===//

class TableFactory {
 public:
  TableFactory();
  virtual ~TableFactory();

  static Table *GetTable(catalog::Manager *catalog,
                         catalog::Schema* schema,
                         int tuple_count,
                         const bool own_schema,
                         Backend* backend = nullptr){

    bool own_backend = false;
    // create backend if needed
    if(backend == nullptr) {
      backend = new storage::VMBackend();
      own_backend = true;
    }

    Table *table = GetTable(INVALID_OID, catalog, schema,
                            backend, tuple_count, own_backend, own_schema);

    if(own_backend) {
      table->own_backend = true;
    }

    return table;
  }

  static Table *GetTable(oid_t database_id,
                         catalog::Manager *catalog,
                         catalog::Schema* schema,
                         Backend* backend,
                         int tuple_count,
                         const bool own_backend,
                         const bool own_schema) {

    Table *table =  new Table(catalog,schema, backend, tuple_count,
                              own_backend, own_schema);

    table->database_id = database_id;

    return table;
  }

};

} // End storage namespace
} // End nstore namespace


