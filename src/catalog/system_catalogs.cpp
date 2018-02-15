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
#include "storage/data_table.h"

namespace peloton {
namespace catalog {

SystemCatalogs::SystemCatalogs(storage::Database *database,
                               type::AbstractPool *pool,
                               concurrency::TransactionContext *txn) {
  pg_attribute = new ColumnCatalog(database, pool, txn);

  // TODO: can we move this to BootstrapSystemCatalogs()?
  oid_t column_id = 0;
  for (auto column :
       storage::StorageManager::GetInstance()
           ->GetTableWithOid(CATALOG_DATABASE_OID, DATABASE_CATALOG_OID)
           ->GetSchema()
           ->GetColumns()) {
    pg_attribute->InsertColumn(DATABASE_CATALOG_OID, column.GetName(),
                               column_id, column.GetOffset(), column.GetType(),
                               column.IsInlined(), column.GetConstraints(),
                               pool, txn);
    column_id++;
  }

  pg_table = new TableCatalog(database, pool, txn);

  pg_index = new IndexCatalog(database, pool, txn);
}

SystemCatalogs::~SystemCatalogs() {
  delete pg_index;
  delete pg_table;
  delete pg_attribute;
}

}  // namespace catalog
}  // namespace peloton
