//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.cpp
//
// Identification: src/catalog/catalog_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog_util.h"


namespace peloton {

namespace catalog {
/**
 * Inserts a tuple in a table
 */
void InsertTuple(storage::DataTable *table, std::unique_ptr<storage::Tuple> tuple) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));
  planner::InsertPlan node(table, std::move(tuple));
  executor::InsertExecutor executor(&node, context.get());
  txn_manager.CommitTransaction();
}

/**
 * Generate a database catalog tuple
 * Input: The database schema, the database id, the database name
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetCatalogTuple(catalog::Schema *schema,
		oid_t id,
		std::string name){
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  Value val1 = ValueFactory::GetIntegerValue(id);
  Value val2 = ValueFactory::GetStringValue(name, nullptr);
  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  return tuple;
}


}
}
