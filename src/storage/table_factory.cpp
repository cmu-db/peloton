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
#include "storage/data_table.h"
#include "storage/storage_manager.h"
#include "storage/temp_table.h"


namespace peloton {
namespace storage {

DataTable *TableFactory::GetDataTable(oid_t database_id, oid_t relation_id,
                                      catalog::Schema *schema,
                                      std::string table_name,
                                      size_t tuples_per_tilegroup_count,
                                      bool own_schema, bool adapt_table,
                                      bool is_catalog,
                                      peloton::LayoutType layout_type) {
  DataTable *table = new DataTable(schema, table_name, database_id, relation_id,
                                   tuples_per_tilegroup_count, own_schema,
                                   adapt_table, is_catalog, layout_type);

  return table;
}

TempTable *TableFactory::GetTempTable(catalog::Schema *schema,
                                      bool own_schema) {
  TempTable *table = new TempTable(INVALID_OID, schema, own_schema);
  return (table);
}

bool TableFactory::DropDataTable(oid_t database_oid, oid_t table_oid) {
  auto storage_manager = storage::StorageManager::GetInstance();
  try {
    DataTable *table = (DataTable *)storage_manager->GetTableWithOid(
        database_oid, table_oid);
    delete table;
  } catch (CatalogException &e) {
    return false;
  }
  return true;
}

}  // namespace storage
}  // namespace peloton
