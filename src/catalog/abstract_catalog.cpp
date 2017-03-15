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
  catalog_table_ = storage::TableFactory::GetDataTable(
      START_OID, catalog_table_id, catalog_table_schema, catalog_table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, true, false, true);

  // Add catalog_table_ into pg_catalog
  catalog::Catalog::GetInstance()->GetCatalogDB()->AddTable(catalog_table_);
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