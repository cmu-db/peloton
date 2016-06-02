//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_factory.cpp
//
// Identification: src/storage/table_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/table_factory.h"

#include "common/exception.h"
#include "common/logger.h"
#include "index/index.h"
#include "catalog/manager.h"
#include "storage/data_table.h"

#include <mutex>

namespace peloton {
namespace storage {

DataTable *TableFactory::GetDataTable(oid_t database_id, oid_t relation_id,
                                      catalog::Schema *schema,
                                      std::string table_name,
                                      size_t tuples_per_tilegroup_count,
                                      bool own_schema, bool adapt_table) {
  DataTable *table =
      new DataTable(schema, table_name, database_id, relation_id,
                    tuples_per_tilegroup_count, own_schema, adapt_table);

  return table;
}

bool TableFactory::DropDataTable(oid_t database_oid, oid_t table_oid) {
  auto &manager = catalog::Manager::GetInstance();
  DataTable *table =
      (DataTable *)manager.GetTableWithOid(database_oid, table_oid);

  if (table == nullptr) return false;

  delete table;
  return true;
}

}  // End storage namespace
}  // End peloton namespace
