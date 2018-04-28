//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_stats_catalog.cpp
//
// Identification: src/catalog/column_stats_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/column_stats_catalog.h"

#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "optimizer/stats/column_stats_collector.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "expression/expression_util.h"
#include "codegen/buffering_consumer.h"

namespace peloton {
namespace catalog {

ColumnStatsCatalog *ColumnStatsCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static ColumnStatsCatalog column_stats_catalog{txn};
  return &column_stats_catalog;
}

ColumnStatsCatalog::ColumnStatsCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." COLUMN_STATS_CATALOG_NAME
                      " ("
                      "database_id    INT NOT NULL, "
                      "table_id       INT NOT NULL, "
                      "column_id      INT NOT NULL, "
                      "num_rows        INT NOT NULL, "
                      "cardinality    DECIMAL NOT NULL, "
                      "frac_null      DECIMAL NOT NULL, "
                      "most_common_vals  VARCHAR, "
                      "most_common_freqs VARCHAR, "
                      "histogram_bounds  VARCHAR, "
                      "column_name       VARCHAR, "
                      "has_index         BOOLEAN);",
                      txn) {
  // unique key: (database_id, table_id, column_id)
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, COLUMN_STATS_CATALOG_NAME, {0, 1, 2},
      COLUMN_STATS_CATALOG_NAME "_skey0", true, IndexType::BWTREE, txn);
  // non-unique key: (database_id, table_id)
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, COLUMN_STATS_CATALOG_NAME, {0, 1},
      COLUMN_STATS_CATALOG_NAME "_skey1", false, IndexType::BWTREE, txn);
}

ColumnStatsCatalog::~ColumnStatsCatalog() {}

bool ColumnStatsCatalog::InsertColumnStats(
    oid_t database_id, oid_t table_id, oid_t column_id, int num_rows,
    double cardinality, double frac_null, std::string most_common_vals,
    std::string most_common_freqs, std::string histogram_bounds,
    std::string column_name, bool has_index, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
  (void) pool;
  std::vector<std::vector<ExpressionPtr>> tuples;

  auto val_db_id = type::ValueFactory::GetIntegerValue(database_id);
  auto val_table_id = type::ValueFactory::GetIntegerValue(table_id);
  auto val_column_id = type::ValueFactory::GetIntegerValue(column_id);
  auto val_num_row = type::ValueFactory::GetIntegerValue(num_rows);
  auto val_cardinality = type::ValueFactory::GetDecimalValue(cardinality);
  auto val_frac_null = type::ValueFactory::GetDecimalValue(frac_null);

  type::Value val_common_val, val_common_freq;
  if (!most_common_vals.empty()) {
    val_common_val = type::ValueFactory::GetVarcharValue(most_common_vals);
    val_common_freq = type::ValueFactory::GetVarcharValue(most_common_freqs);
  } else {
    val_common_val =
        type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    val_common_freq =
        type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }

  type::Value val_hist_bounds;
  if (!histogram_bounds.empty()) {
    val_hist_bounds = type::ValueFactory::GetVarcharValue(histogram_bounds);
  } else {
    val_hist_bounds =
        type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }

  type::Value val_column_name =
      type::ValueFactory::GetVarcharValue(column_name);
  type::Value val_has_index = type::ValueFactory::GetBooleanValue(has_index);

  tuples.push_back(std::vector<ExpressionPtr>());
  auto &values = tuples[0];

  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_db_id)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_table_id)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_column_id)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_num_row)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_cardinality)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_frac_null)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_common_val)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_common_freq)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_hist_bounds)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_column_name)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(val_has_index)));

  // Insert the tuple into catalog table
  return InsertTupleWithCompiledPlan(&tuples, txn);
}

bool ColumnStatsCatalog::DeleteColumnStats(
    oid_t database_id, oid_t table_id, oid_t column_id,
    concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids(all_column_ids);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::DATABASE_ID);
  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::DATABASE_ID);
  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_id).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);

  auto *tb_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::TABLE_ID);
  tb_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::TABLE_ID);
  expression::AbstractExpression *tb_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_id).Copy());
  expression::AbstractExpression *tb_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, tb_oid_expr, tb_oid_const_expr);

  auto *col_id_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::COLUMN_ID);
  col_id_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::COLUMN_ID);
  expression::AbstractExpression *col_id_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(column_id).Copy());
  expression::AbstractExpression *col_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, col_id_expr, col_id_const_expr);

  expression::AbstractExpression *db_and_tb =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, db_oid_equality_expr,
          tb_oid_equality_expr);
  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, db_and_tb, col_oid_equality_expr);



  return DeleteWithCompiledSeqScan(column_ids, predicate, txn);
}

std::unique_ptr<std::vector<type::Value>> ColumnStatsCatalog::GetColumnStats(
    oid_t database_id, oid_t table_id, oid_t column_id,
    concurrency::TransactionContext *txn) {

  std::vector<oid_t> column_ids(all_column_ids);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                                    ColumnId::DATABASE_ID);
  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::DATABASE_ID);
  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_id).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);

  auto *tb_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                                    ColumnId::TABLE_ID);
  tb_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::TABLE_ID);
  expression::AbstractExpression *tb_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_id).Copy());
  expression::AbstractExpression *tb_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, tb_oid_expr, tb_oid_const_expr);

  auto *col_id_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                                    ColumnId::COLUMN_ID);
  col_id_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::COLUMN_ID);
  expression::AbstractExpression *col_id_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(column_id).Copy());
  expression::AbstractExpression *col_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, col_id_expr, col_id_const_expr);

  expression::AbstractExpression *db_and_tb =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, db_oid_equality_expr,
          tb_oid_equality_expr);
  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, db_and_tb, col_oid_equality_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  PELOTON_ASSERT(result_tuples.size() <= 1);  // unique
  if (result_tuples.size() == 0) {
    return nullptr;
  }

  codegen::WrappedTuple tuple = result_tuples[0];

  type::Value num_rows, cardinality, frac_null, most_common_vals,
      most_common_freqs, hist_bounds, column_name, has_index;

  num_rows = tuple.GetValue(ColumnId::NUM_ROWS);
  cardinality = tuple.GetValue(ColumnId::CARDINALITY);
  frac_null = tuple.GetValue(ColumnId::FRAC_NULL);
  most_common_vals = tuple.GetValue(ColumnId::MOST_COMMON_VALS);
  most_common_freqs = tuple.GetValue(ColumnId::MOST_COMMON_FREQS);
  hist_bounds = tuple.GetValue(ColumnId::HISTOGRAM_BOUNDS);
  column_name = tuple.GetValue(ColumnId::COLUMN_NAME);
  has_index = tuple.GetValue(ColumnId::HAS_INDEX);

  std::unique_ptr<std::vector<type::Value>> column_stats(
      new std::vector<type::Value>({num_rows, cardinality, frac_null,
                                    most_common_vals, most_common_freqs,
                                    hist_bounds, column_name, has_index}));

  return column_stats;
}

// Return value: number of column stats
size_t ColumnStatsCatalog::GetTableStats(
    oid_t database_id, oid_t table_id, concurrency::TransactionContext *txn,
    std::map<oid_t, std::unique_ptr<std::vector<type::Value>>> &
        column_stats_map) {
  std::vector<oid_t> column_ids(all_column_ids);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                                    ColumnId::DATABASE_ID);
  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::DATABASE_ID);
  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_id).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr,
          db_oid_const_expr);

  auto *tb_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                                    ColumnId::TABLE_ID);
  tb_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::TABLE_ID);
  expression::AbstractExpression *tb_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_id).Copy());
  expression::AbstractExpression *tb_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, tb_oid_expr, tb_oid_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, db_oid_equality_expr,
          tb_oid_equality_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  size_t tuple_count = result_tuples.size();
  LOG_DEBUG("Tuple count: %lu", tuple_count);
  if (tuple_count == 0) {
    return 0;
  }

  type::Value num_rows, cardinality, frac_null, most_common_vals,
      most_common_freqs, hist_bounds, column_name, has_index;
  for (auto tuple : result_tuples) {
    num_rows = tuple.GetValue(ColumnId::NUM_ROWS);
    cardinality = tuple.GetValue(ColumnId::CARDINALITY);
    frac_null = tuple.GetValue(ColumnId::FRAC_NULL);
    most_common_vals = tuple.GetValue(ColumnId::MOST_COMMON_VALS);
    most_common_freqs = tuple.GetValue(ColumnId::MOST_COMMON_FREQS);
    hist_bounds = tuple.GetValue(ColumnId::HISTOGRAM_BOUNDS);
    column_name = tuple.GetValue(ColumnId::COLUMN_NAME);
    has_index = tuple.GetValue(ColumnId::HAS_INDEX);

    std::unique_ptr<std::vector<type::Value>> column_stats(
        new std::vector<type::Value>({num_rows, cardinality, frac_null,
                                      most_common_vals, most_common_freqs,
                                      hist_bounds, column_name, has_index}));

    oid_t column_id = tuple.GetValue(ColumnId::COLUMN_ID).GetAs<int>();
    column_stats_map[column_id] = std::move(column_stats);
  }
  return tuple_count;
}

}  // namespace catalog
}  // namespace peloton
