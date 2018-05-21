//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_catalog.cpp
//
// Identification: src/catalog/settings_catalog.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/expression_util.h"
#include "codegen/buffering_consumer.h"
#include "catalog/settings_catalog.h"
#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

#define SETTINGS_CATALOG_NAME "pg_settings"

namespace peloton {
namespace catalog {

SettingsCatalog &SettingsCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static SettingsCatalog settings_catalog{txn};
  return settings_catalog;
}

SettingsCatalog::SettingsCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." CATALOG_SCHEMA_NAME "." SETTINGS_CATALOG_NAME
                      " ("
                      "name   VARCHAR NOT NULL, "
                      "value  VARCHAR NOT NULL, "
                      "value_type   VARCHAR NOT NULL, "
                      "description  VARCHAR, "
                      "min_value    VARCHAR, "
                      "max_value    VARCHAR, "
                      "default_value    VARCHAR NOT NULL, "
                      "is_mutable   BOOL NOT NULL, "
                      "is_persistent  BOOL NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, CATALOG_SCHEMA_NAME, SETTINGS_CATALOG_NAME, {0},
      SETTINGS_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);
}

SettingsCatalog::~SettingsCatalog() {}

bool SettingsCatalog::InsertSetting(
    const std::string &name, const std::string &value, type::TypeId value_type,
    const std::string &description, const std::string &min_value,
    const std::string &max_value, const std::string &default_value,
    bool is_mutable, bool is_persistent, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
  (void) pool;
  // Create the tuple first
  std::vector<std::vector<ExpressionPtr>> tuples;

  auto val0 = type::ValueFactory::GetVarcharValue(name, pool);
  auto val1 = type::ValueFactory::GetVarcharValue(value, pool);
  auto val2 =
      type::ValueFactory::GetVarcharValue(TypeIdToString(value_type), pool);
  auto val3 = type::ValueFactory::GetVarcharValue(description, pool);
  auto val4 = type::ValueFactory::GetVarcharValue(min_value, pool);
  auto val5 = type::ValueFactory::GetVarcharValue(max_value, pool);
  auto val6 = type::ValueFactory::GetVarcharValue(default_value, pool);
  auto val7 = type::ValueFactory::GetBooleanValue(is_mutable);
  auto val8 = type::ValueFactory::GetBooleanValue(is_persistent);

  auto constant_expr_0 = new expression::ConstantValueExpression(
    val0);
  auto constant_expr_1 = new expression::ConstantValueExpression(
    val1);
  auto constant_expr_2 = new expression::ConstantValueExpression(
    val2);
  auto constant_expr_3 = new expression::ConstantValueExpression(
    val3);
  auto constant_expr_4 = new expression::ConstantValueExpression(
    val4);
  auto constant_expr_5 = new expression::ConstantValueExpression(
    val5);
  auto constant_expr_6 = new expression::ConstantValueExpression(
    val6);
  auto constant_expr_7 = new expression::ConstantValueExpression(
    val7);
  auto constant_expr_8 = new expression::ConstantValueExpression(
    val8);

  tuples.push_back(std::vector<ExpressionPtr>());
  auto &values = tuples[0];
  values.push_back(ExpressionPtr(constant_expr_0));
  values.push_back(ExpressionPtr(constant_expr_1));
  values.push_back(ExpressionPtr(constant_expr_2));
  values.push_back(ExpressionPtr(constant_expr_3));
  values.push_back(ExpressionPtr(constant_expr_4));
  values.push_back(ExpressionPtr(constant_expr_5));
  values.push_back(ExpressionPtr(constant_expr_6));
  values.push_back(ExpressionPtr(constant_expr_7));
  values.push_back(ExpressionPtr(constant_expr_8));

  // Insert the tuple
  return InsertTupleWithCompiledPlan(&tuples, txn);
}

bool SettingsCatalog::DeleteSetting(const std::string &name,
                                    concurrency::TransactionContext *txn) {
 std::vector<oid_t> column_ids(all_column_ids);

 auto *name_expr =
   new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        static_cast<int>(ColumnId::NAME));
 name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), static_cast<int>(ColumnId::NAME));

 expression::AbstractExpression *name_const_expr =
   expression::ExpressionUtil::ConstantValueFactory(
     type::ValueFactory::GetVarcharValue(name).Copy());
 expression::AbstractExpression *name_equality_expr =
   expression::ExpressionUtil::ComparisonFactory(
     ExpressionType::COMPARE_EQUAL, name_expr, name_const_expr);

 return DeleteWithCompiledSeqScan(column_ids, name_equality_expr, txn);
}

std::string SettingsCatalog::GetSettingValue(
    const std::string &name, concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids(all_column_ids);

  auto *name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                           static_cast<int>(ColumnId::NAME));

  name_expr->SetBoundOid(
      catalog_table_->GetDatabaseOid(),
      catalog_table_->GetOid(),
      static_cast<int>(ColumnId::NAME));

  expression::AbstractExpression *name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  expression::AbstractExpression *name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, name_expr, name_const_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, name_equality_expr, txn);

  std::string config_value = "";
  PELOTON_ASSERT(result_tuples.size() <= 1);
  if (!result_tuples.empty()) {
    config_value = (result_tuples[0]).GetValue(static_cast<int>(ColumnId::VALUE)).ToString();
  }

  return config_value;
}

std::string SettingsCatalog::GetDefaultValue(
    const std::string &name, concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids(all_column_ids);

  auto *name_expr =
      new expression::TupleValueExpression(
          type::TypeId::VARCHAR, 0,
          static_cast<int>(ColumnId::NAME));

  name_expr->SetBoundOid(
      catalog_table_->GetDatabaseOid(),
      catalog_table_->GetOid(),
      static_cast<int>(ColumnId::NAME));

  expression::AbstractExpression *name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  expression::AbstractExpression *name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, name_expr, name_const_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, name_equality_expr, txn);

  std::string config_value = "";
  PELOTON_ASSERT(result_tuples.size() <= 1);
  if (!result_tuples.empty()) {
    config_value = result_tuples[0].GetValue(static_cast<int>(ColumnId::DEFAULT_VALUE)).ToString();
  }
  return config_value;
}

}  // namespace catalog
}  // namespace peloton
