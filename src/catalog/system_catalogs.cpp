//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// system_catalog.cpp
//
// Identification: src/catalog/system_catalog.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/system_catalogs.h"
#include "catalog/column_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/index_catalog.h"
#include "storage/storage_manager.h"
#include "storage/database.h"
#include "storage/data_table.h"

namespace peloton {
namespace catalog {

SystemCatalogs::SystemCatalogs(storage::Database *database,
                               type::AbstractPool *pool,
                               concurrency::TransactionContext *txn) {
  oid_t database_oid = database->GetOid();
  pg_attribute = new ColumnCatalog(database, pool, txn);
  pg_table = new TableCatalog(database, pool, txn);
  pg_index = new IndexCatalog(database, pool, txn);

  // TODO: can we move this to BootstrapSystemCatalogs()?
  // insert pg_database columns into pg_attribute
  std::vector<std::pair<oid, oid>> shared_tables = {
      {CATALOG_DATABASE_OID, DATABASE_CATALOG_OID},
      {database_oid, TABLE_CATALOG_OID},
      {database_oid, INDEX_CATALOG_OID}};

  for (int i = 0; i < shared_tables.size(); i++) {
    oid_t column_id = 0;
    for (auto column :
         storage::StorageManager::GetInstance()
             ->GetTableWithOid(CATALOG_DATABASE_OID, DATABASE_CATALOG_OID)
             ->GetSchema()
             ->GetColumns()) {
      pg_attribute->InsertColumn(DATABASE_CATALOG_OID, column.GetName(),
                                 column_id, column.GetOffset(),
                                 column.GetType(), column.IsInlined(),
                                 column.GetConstraints(), pool, txn);
      column_id++;
    }
  }
}

SystemCatalogs::~SystemCatalogs() {
  delete pg_index;
  delete pg_table;
  delete pg_attribute;
}

}  // namespace catalog
}  // namespace peloton
