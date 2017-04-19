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

#include "catalog/catalog_defaults.h"
#include "catalog/schema.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/delete_executor.h"
#include "executor/executor_context.h"
#include "executor/index_scan_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/insert_executor.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "index/index_factory.h"
#include "planner/delete_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "type/types.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

class AbstractCatalog {
 public:
  virtual ~AbstractCatalog() {}

 protected:
  /* For pg_database, pg_table, pg_index, pg_column */
  AbstractCatalog(oid_t catalog_table_oid, std::string catalog_table_name,
                  catalog::Schema *catalog_table_schema,
                  storage::Database *pg_catalog);

  /* For other catalogs */
  AbstractCatalog(const std::string &catalog_table_ddl,
                  concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Helper Functions
  //===--------------------------------------------------------------------===//
  bool InsertTuple(std::unique_ptr<storage::Tuple> tuple,
                   concurrency::Transaction *txn);

  bool DeleteWithIndexScan(oid_t index_offset, std::vector<type::Value> values,
                           concurrency::Transaction *txn);

  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
  GetResultWithIndexScan(std::vector<oid_t> column_offsets, oid_t index_offset,
                         std::vector<type::Value> values,
                         concurrency::Transaction *txn);

  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
  GetResultWithSeqScan(std::vector<oid_t> column_offsets,
                       expression::AbstractExpression *predicate,
                       concurrency::Transaction *txn);

  void AddIndex(const std::vector<oid_t> &key_attrs, oid_t index_oid,
                const std::string &index_name,
                IndexConstraintType index_constraint);

  //===--------------------------------------------------------------------===//
  // Memebers
  //===--------------------------------------------------------------------===//
  // Maximum column name size for catalog schemas
  static const size_t max_name_size = 32;

  // Local oid (without catalog type mask) starts from START_OID + OID_OFFSET
  std::atomic<oid_t> oid_ = ATOMIC_VAR_INIT(START_OID + OID_OFFSET);

  storage::DataTable *catalog_table_;
};

}  // End catalog namespace
}  // End peloton namespace
