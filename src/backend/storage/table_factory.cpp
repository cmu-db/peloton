//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// table_factory.cpp
//
// Identification: src/backend/storage/table_factory.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/table_factory.h"

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/catalog/manager.h"
#include "backend/storage/data_table.h"

#include <mutex>

namespace peloton {
namespace storage {

DataTable *TableFactory::GetDataTable(oid_t database_id, oid_t relation_id,
                                      catalog::Schema *schema,
                                      std::string table_name,
                                      size_t tuples_per_tilegroup_count,
                                      bool own_schema) {
  // create a new backend
  // FIXME: We need a better way of managing these. Why not just embed it in
  //        directly inside of the table object?
  AbstractBackend *backend = new VMBackend();

  DataTable *table = new DataTable(schema, backend, table_name, database_id, relation_id,
                                   tuples_per_tilegroup_count, own_schema);

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
