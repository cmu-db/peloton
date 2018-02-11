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

namespace peloton {
namespace catalog {

SystemCatalogs::SystemCatalogs(storage::Database *database,
                               type::AbstractPool *pool,
                               concurrency::TransactionContext *txn) {
  pg_attribute = new ColumnCatalog(database, pool, txn);

  oid_t column_id = 0;
  for (auto column :
       StorageManager::GetInstance()
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
  delete IndexCatalog;
  delete TableCatalog;
  delete ColumnCatalog;
}

}  // namespace catalog
}  // namespace peloton
