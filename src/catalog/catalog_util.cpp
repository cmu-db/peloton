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

void InsertTuple(storage::DataTable *table, std::unique_ptr<storage::Tuple> tuple) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));
  planner::InsertPlan node(table, std::move(tuple));
  executor::InsertExecutor executor(&node, context.get());
  txn_manager.CommitTransaction();
}

std::unique_ptr<storage::Tuple> GetDatabaseCatalogTuple(catalog::Schema *schema,
		oid_t database_id,
		std::string database_name){

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));

  Value val1 = ValueFactory::GetIntegerValue(database_id);
  Value val2 = ValueFactory::GetStringValue(database_name, nullptr);
  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  return tuple;
}

std::unique_ptr<storage::Tuple> GetTableCatalogTuple(catalog::Schema *schema,
		oid_t table_id,
		std::string table_name){
	std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
	tuple->SetValue(0, ValueFactory::GetIntegerValue(table_id), nullptr);
	tuple->SetValue(1, ValueFactory::GetStringValue(table_name), nullptr);
	return tuple;
}

}
}
