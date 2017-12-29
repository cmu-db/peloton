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

#include <string>

#include "catalog/manager.h"
#include "common/internal_types.h"
#include "storage/data_table.h"
#include "storage/temp_table.h"

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
  static DataTable *GetDataTable(oid_t database_id, oid_t table_id,
                                 catalog::Schema *schema,
                                 std::string table_name,
                                 size_t tuples_per_tile_group_count,
                                 bool own_schema, bool adapt_table,
                                 bool is_catalog = false,
                                 peloton::LayoutType layout_type = peloton::LayoutType::ROW);

  static TempTable *GetTempTable(catalog::Schema *schema, bool own_schema);

  /**
   * For a given table name, drop the table from database
   */
  static bool DropDataTable(oid_t database_oid, oid_t table_oid);
};

}  // namespace storage
}  // namespace peloton
