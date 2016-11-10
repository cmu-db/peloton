//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_util.h
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

#include "planner/delete_plan.h"
#include "executor/delete_executor.h"
#include "expression/tuple_value_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/comparison_expression.h"
#include "planner/seq_scan_plan.h"
#include "executor/seq_scan_executor.h"

namespace peloton {

namespace catalog {

void InsertTuple(storage::DataTable *table,
                 std::unique_ptr<storage::Tuple> tuple,
                 concurrency::Transaction *txn);

void DeleteTuple(storage::DataTable *table, oid_t id,
                 concurrency::Transaction *txn);

std::unique_ptr<storage::Tuple> GetDatabaseCatalogTuple(
    catalog::Schema *schema, oid_t database_id, std::string database_name,
    common::VarlenPool *pool);

std::unique_ptr<storage::Tuple> GetTableCatalogTuple(
    catalog::Schema *schema, oid_t table_id, std::string table_name,
    oid_t database_id, std::string database_name, common::VarlenPool *pool);

std::unique_ptr<storage::Tuple> GetDatabaseMetricsCatalogTuple(
    catalog::Schema *schema, oid_t database_id, int64_t commit, int64_t abort,
    int64_t time);

std::unique_ptr<storage::Tuple> GetTableMetricsCatalogTuple(
    catalog::Schema *schema, oid_t database_id, oid_t table_id, int64_t reads,
    int64_t updates, int64_t deletes, int64_t inserts, int64_t time);

std::unique_ptr<storage::Tuple> GetIndexMetricsCatalogTuple(
    catalog::Schema *schema, oid_t database_id, oid_t table_id, oid_t index_id,
    int64_t reads, int64_t deletes, int64_t inserts, int64_t time);

std::unique_ptr<storage::Tuple> GetQueryMetricsCatalogTuple(
    catalog::Schema *schema, std::string query_name, oid_t database_id,
    int64_t num_params, stats::QueryMetric::QueryParamBuf type_buf,
    stats::QueryMetric::QueryParamBuf format_buf,
    stats::QueryMetric::QueryParamBuf val_buf, int64_t reads, int64_t updates,
    int64_t deletes, int64_t inserts, int64_t latency, int64_t cpu_time,
    int64_t time_stamp, common::VarlenPool *pool);
}
}
