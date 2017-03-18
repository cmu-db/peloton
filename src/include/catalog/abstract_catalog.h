//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_catalog.h
//
// Identification: src/include/catalog/abstract_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/delete_executor.h"
#include "executor/executor_context.h"
#include "executor/insert_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/index_scan_executor.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/index_scan_plan.h"
#include "storage/data_table.h"
#include "storage/data_table.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/database.h"
#include "storage/table_factory.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "storage/tuple.h"
#include "type/catalog_type.h"
#include "type/types.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

class AbstractCatalog {
 public:
  virtual oid_t GetNextOid() = 0;

 protected:
  AbstractCatalog(oid_t catalog_table_id, std::string catalog_table_name,
                  catalog::Schema *catalog_table_schema);

  virtual ~AbstractCatalog() {}

  bool InsertTuple(std::unique_ptr<storage::Tuple> tuple,
                   concurrency::Transaction *txn);

  // Maximum column name size for catalog schemas
  const size_t max_name_size = 32;

  // Local oid (without catalog type mask) starts from START_OID + OID_OFFSET
  std::atomic<oid_t> oid_ = ATOMIC_VAR_INIT(START_OID + OID_OFFSET);

  std::shared_ptr<storage::DataTable> catalog_table_;
};

}  // End catalog namespace
}  // End peloton namespace
