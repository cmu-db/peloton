//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// table_factory.h
//
// Identification: src/backend/storage/table_factory.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/storage/data_table.h"

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
  static DataTable *GetDataTable(
      oid_t database_id, oid_t relation_id, catalog::Schema *schema,
      std::string table_name,
      size_t tuples_per_tile_group_count,
      bool own_schema,
      bool adapt_table);

  /**
   * For a given table name, drop the table from database
   */
  static bool DropDataTable(oid_t database_oid, oid_t table_oid);
};

}  // End storage namespace
}  // End peloton namespace
