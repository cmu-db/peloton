//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_catalog.cpp
//
// Identification: src/catalog/abstract_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

AbstractCatalog::AbstractCatalog(oid_t catalog_table_id,
                                 std::string catalog_table_name,
                                 catalog::Schema *catalog_table_schema) {
  catalog_table_ =
      std::shared_ptr<storage::DataTable>(storage::TableFactory::GetDataTable(
          START_OID, catalog_table_id, catalog_table_schema, catalog_table_name,
          DEFAULT_TUPLES_PER_TILEGROUP, true, false, true));

  // Add catalog_table_ into pg_catalog
  auto pg_catalog = catalog::Catalog::GetInstance()->GetCatalogDB();
  pg_catalog->AddTable(catalog_table_.get());

  // Create primary index for catalog_table_
  std::vector<oid_t> key_attrs;
  int column_idx = 0;
  for (auto &column : catalog_table_schema->GetColumns()) {
    if (column.IsPrimary()) {
      key_attrs.push_back(column_idx);
    }
    column_idx++;
  }
}

void AbstractCatalog::CreateIndex(std::vector<oid_t> key_attrs) {
  catalog::Schema *key_schema = nullptr;
  index::IndexMetadata *index_metadata = nullptr;

  key_schema = catalog::Schema::CopySchema(catalog_table_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  std::string index_name = table->GetName() + "_PKEY";

  bool unique_keys = true;

  index_metadata = new index::IndexMetadata(
      StringUtil::Upper(index_name), GetNextOid(), table->GetOid(),
      database->GetOid(), IndexType::BWTREE, IndexConstraintType::PRIMARY_KEY,
      catalog_table_schema, key_schema, key_attrs, unique_keys);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));
  table->AddIndex(pkey_index);
}

void AbstractCatalog::InsertTuple(std::unique_ptr<storage::Tuple> tuple,
                                  concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::InsertPlan node(catalog_table_.get(), std::move(tuple));
  executor::InsertExecutor executor(&node, context.get());
  executor.Init();
  executor.Execute();

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }
}

}  // End catalog namespace
}  // End peloton namespace