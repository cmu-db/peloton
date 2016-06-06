//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_factory.h
//
// Identification: src/include/storage/table_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/manager.h"
#include "common/types.h"
#include "storage/data_table.h"

#include <iostream>
#include <map>
#include <string>

namespace peloton {
namespace storage {

/**
 * Magic Table Factory!!
 */
class TableFactory {
 public:
  /**
   * For a given Schema, instantiate a DataTable object and return it
   */
  static DataTable *GetDataTable(oid_t database_id, oid_t relation_id,
                                 catalog::Schema *schema,
                                 std::string table_name,
                                 size_t tuples_per_tile_group_count,
                                 bool own_schema, bool adapt_table);

  /**
   * For a given table name, drop the table from database
   */
  static bool DropDataTable(oid_t database_oid, oid_t table_oid);
};

}  // End storage namespace
}  // End peloton namespace
