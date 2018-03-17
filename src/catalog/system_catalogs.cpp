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
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/storage_manager.h"

namespace peloton {
namespace catalog {

SystemCatalogs::SystemCatalogs(storage::Database *database,
                               type::AbstractPool *pool,
                               concurrency::TransactionContext *txn)
    : pg_trigger(nullptr) {
  oid_t database_oid = database->GetOid();
  pg_attribute = new ColumnCatalog(database, pool, txn);
  pg_table = new TableCatalog(database, pool, txn);
  pg_index = new IndexCatalog(database, pool, txn);

  // TODO: can we move this to BootstrapSystemCatalogs()?
  std::vector<std::pair<oid_t, oid_t>> shared_tables = {
      {CATALOG_DATABASE_OID, DATABASE_CATALOG_OID},
      {database_oid, TABLE_CATALOG_OID},
      {database_oid, INDEX_CATALOG_OID}};

  for (int i = 0; i < (int)shared_tables.size(); i++) {
    oid_t column_id = 0;
    for (auto column :
         storage::StorageManager::GetInstance()
             ->GetTableWithOid(shared_tables[i].first, shared_tables[i].second)
             ->GetSchema()
             ->GetColumns()) {
      pg_attribute->InsertColumn(shared_tables[i].second, column.GetName(),
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
  if (pg_trigger) delete pg_trigger;
}

void SystemCatalogs::Bootstrap(const std::string &database_name,
                               concurrency::TransactionContext *txn) {
  pg_trigger = new TriggerCatalog(database_name, txn);
}

}  // namespace catalog
}  // namespace peloton
