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
  Table(const catalog::Schema& schema,
        Backend *backend,
        size_t tuple_count)
 : database_id(INVALID_ID),
   table_id(INVALID_ID),
   backend(backend),
   schema(schema),
   tuple_count(tuple_count) {
  }

  ~Table() {

    // clean up tile groups
    for(auto tile_group : tile_groups)
      delete tile_group;

  }

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  // add a new tile group to table
  oid_t AddTileGroup();

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
  catalog::Schema schema;

  // set of tile groups
  std::vector<TileGroup*> tile_groups;

  size_t tuple_count;

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
                         Backend* backend,
                         catalog::Schema schema,
                         size_t tuple_count) {

    Table *table =  new Table(schema, backend, tuple_count);

    table->database_id = database_id;

    return table;
  }

};

} // End storage namespace
} // End nstore namespace


