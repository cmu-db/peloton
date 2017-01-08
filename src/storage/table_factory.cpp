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

#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/logger.h"
#include "index/index.h"
#include "storage/data_table.h"
#include "storage/temp_table.h"

#include <mutex>

namespace peloton {
namespace storage {

DataTable *TableFactory::GetDataTable(oid_t database_id, oid_t relation_id,
                                      catalog::Schema *schema,
                                      std::string table_name,
                                      size_t tuples_per_tilegroup_count,
                                      bool own_schema, bool adapt_table, bool is_catalog) {
  DataTable *table =
      new DataTable(schema, table_name, database_id, relation_id,
                    tuples_per_tilegroup_count, own_schema, adapt_table, is_catalog);

  return table;
}

TempTable *TableFactory::GetTempTable(catalog::Schema *schema,
                                      bool own_schema) {
  TempTable *table = new TempTable(INVALID_OID, schema, own_schema);
  return (table);
}

bool TableFactory::DropDataTable(oid_t database_oid, oid_t table_oid) {
  auto catalog = catalog::Catalog::GetInstance();
  try {
    DataTable *table =
        (DataTable *)catalog->GetTableWithOid(database_oid, table_oid);
    delete table;
  } catch (CatalogException &e) {
    return false;
  }
  return true;
}

}  // End storage namespace
}  // End peloton namespace
