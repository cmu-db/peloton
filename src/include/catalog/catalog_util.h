//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.h
//
// Identification: src/include/catalog/catalog_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include "common/types.h"
#include "catalog/schema.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "storage/table_factory.h"
#include "common/value_factory.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "planner/insert_plan.h"
#include "executor/insert_executor.h"

namespace peloton {

namespace catalog {

void InsertTuple(storage::DataTable *table, std::unique_ptr<storage::Tuple> tuple);

std::unique_ptr<storage::Tuple> GetDatabaseCatalogTuple(catalog::Schema *schema, oid_t database_id, std::string database_name);

std::unique_ptr<storage::Tuple> GetTableCatalogTuple(catalog::Schema *schema, oid_t table_id, std::string table_name);

}
}
