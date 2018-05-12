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

/*@brief    system catalog constructor, create core catalog tables and manually
 * insert records into pg_attribute
 * @param   database    the database which the catalog tables belongs to
 * @param   txn         TransactionContext
 */
SystemCatalogs::SystemCatalogs(storage::Database *database,
                               type::AbstractPool *pool,
                               concurrency::TransactionContext *txn)
    : pg_trigger_(nullptr),
      pg_table_metrics_(nullptr),
      pg_index_metrics_(nullptr),
      pg_query_metrics_(nullptr) {
  oid_t database_oid = database->GetOid();
  pg_attribute_ = new ColumnCatalog(database, pool, txn);
  pg_namespace_ = new SchemaCatalog(database, pool, txn);
  pg_table_ = new TableCatalog(database, pool, txn);
  pg_index_ = new IndexCatalog(database, pool, txn);

  // TODO: can we move this to BootstrapSystemCatalogs()?
  // insert column information into pg_attribute
  std::vector<std::pair<oid_t, oid_t>> shared_tables = {
      {CATALOG_DATABASE_OID, DATABASE_CATALOG_OID},
      {database_oid, TABLE_CATALOG_OID},
      {database_oid, SCHEMA_CATALOG_OID},
      {database_oid, INDEX_CATALOG_OID}};

  for (int i = 0; i < (int)shared_tables.size(); i++) {
    oid_t column_id = 0;
    for (auto column :
         storage::StorageManager::GetInstance()
             ->GetTableWithOid(shared_tables[i].first, shared_tables[i].second)
             ->GetSchema()
             ->GetColumns()) {
      pg_attribute_->InsertColumn(shared_tables[i].second, column.GetName(),
                                  column_id, column.GetOffset(),
                                  column.GetType(), column.IsInlined(),
                                  column.GetConstraints(), pool, txn);
      column_id++;
    }
  }
}

SystemCatalogs::~SystemCatalogs() {
  delete pg_index_;
  delete pg_table_;
  delete pg_attribute_;
  delete pg_namespace_;
  if (pg_trigger_) delete pg_trigger_;
  // if (pg_proc) delete pg_proc;
  if (pg_table_metrics_) delete pg_table_metrics_;
  if (pg_index_metrics_) delete pg_index_metrics_;
  if (pg_query_metrics_) delete pg_query_metrics_;
}

/*@brief    using sql create statement to create secondary catalog tables
 * @param   database_name    the database which the namespace belongs to
 * @param   txn              TransactionContext
 */
void SystemCatalogs::Bootstrap(const std::string &database_name,
                               concurrency::TransactionContext *txn) {
  LOG_DEBUG("Bootstrapping database: %s", database_name.c_str());

  if (!pg_trigger_) {
    pg_trigger_ = new TriggerCatalog(database_name, txn);
  }

  // if (!pg_proc) {
  //     pg_proc = new ProcCatalog(database_name, txn);
  // }

  if (!pg_table_metrics_) {
    pg_table_metrics_ = new TableMetricsCatalog(database_name, txn);
  }

  if (!pg_index_metrics_) {
    pg_index_metrics_ = new IndexMetricsCatalog(database_name, txn);
  }

  if (!pg_query_metrics_) {
    pg_query_metrics_ = new QueryMetricsCatalog(database_name, txn);
  }
}

}  // namespace catalog
}  // namespace peloton
