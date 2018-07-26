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
#include "catalog/layout_catalog.h"
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
SystemCatalogs::SystemCatalogs(concurrency::TransactionContext *txn,
                               storage::Database *database,
                               type::AbstractPool *pool)
    : pg_trigger_(nullptr),
      pg_table_metrics_(nullptr),
      pg_index_metrics_(nullptr),
      pg_query_metrics_(nullptr) {
  oid_t database_oid = database->GetOid();

  pg_attribute_ = new ColumnCatalog(txn, database, pool);
  pg_namespace_ = new SchemaCatalog(txn, database, pool);
  pg_table_ = new TableCatalog(txn, database, pool);
  pg_index_ = new IndexCatalog(txn, database, pool);
  pg_layout_ = new LayoutCatalog(txn, database, pool);
  pg_constraint_ = new ConstraintCatalog(txn, database, pool);

  // TODO: can we move this to BootstrapSystemCatalogs()?
  // insert column information into pg_attribute
  // and insert constraint information into pg_constraint
  std::vector<std::pair<oid_t, oid_t>> shared_tables = {
      {CATALOG_DATABASE_OID, DATABASE_CATALOG_OID},
      {database_oid, TABLE_CATALOG_OID},
      {database_oid, SCHEMA_CATALOG_OID},
      {database_oid, INDEX_CATALOG_OID},
      {database_oid, LAYOUT_CATALOG_OID},
      {database_oid, CONSTRAINT_CATALOG_OID}};

  for (int i = 0; i < (int)shared_tables.size(); i++) {
    auto schema = storage::StorageManager::GetInstance()
       ->GetTableWithOid(shared_tables[i].first, shared_tables[i].second)
       ->GetSchema();
    oid_t column_id = 0;
    for (auto column : schema->GetColumns()) {
      pg_attribute_->InsertColumn(txn,
                                  shared_tables[i].second,
                                  column.GetName(),
                                  column_id,
                                  column.GetOffset(),
                                  column.GetType(),
                                  column.GetLength(),
                                  column.IsInlined(),
                                  column.IsNotNull(),
                                  column.HasDefault(),
                                  column.GetDefaultValue(),
                                  pool);
      column_id++;
    }
    for (auto constraint : schema->GetConstraints()) {
      pg_constraint_->InsertConstraint(txn, constraint.second, pool);
    }
  }
}

SystemCatalogs::~SystemCatalogs() {
  delete pg_index_;
  delete pg_layout_;
  delete pg_table_;
  delete pg_attribute_;
  delete pg_namespace_;
  delete pg_constraint_;
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
void SystemCatalogs::Bootstrap(concurrency::TransactionContext *txn,
                               const std::string &database_name) {
  LOG_DEBUG("Bootstrapping database: %s", database_name.c_str());

  if (!pg_trigger_) {
    pg_trigger_ = new TriggerCatalog(txn, database_name);
  }

  // if (!pg_proc) {
  //     pg_proc = new ProcCatalog(database_name, txn);
  // }

  if (!pg_table_metrics_) {
    pg_table_metrics_ = new TableMetricsCatalog(txn, database_name);
  }

  if (!pg_index_metrics_) {
    pg_index_metrics_ = new IndexMetricsCatalog(txn, database_name);
  }

  if (!pg_query_metrics_) {
    pg_query_metrics_ = new QueryMetricsCatalog(txn, database_name);
  }

  // Reset oid of each catalog to avoid collisions between catalog
  // values added by system and users when checkpoint recovery.
  pg_attribute_->UpdateOid(OID_FOR_USER_OFFSET);
  pg_namespace_->UpdateOid(OID_FOR_USER_OFFSET);
  pg_table_->UpdateOid(OID_FOR_USER_OFFSET);
  pg_index_->UpdateOid(OID_FOR_USER_OFFSET);
  pg_constraint_->UpdateOid(OID_FOR_USER_OFFSET);
  pg_trigger_->UpdateOid(OID_FOR_USER_OFFSET);
  // pg_proc->UpdateOid(OID_FOR_USER_OFFSET);
}

}  // namespace catalog
}  // namespace peloton
