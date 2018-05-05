//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// language_catalog.cpp
//
// Identification: src/catalog/language_catalog.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/language_catalog.h"

#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"
#include "codegen/buffering_consumer.h"
#include "expression/expression_util.h"

namespace peloton {
namespace catalog {

LanguageCatalogObject::LanguageCatalogObject(codegen::WrappedTuple tuple)
    : lang_oid_(tuple.GetValue(0).GetAs<oid_t>()),
      lang_name_(tuple.GetValue(1).GetAs<const char *>()) {}

LanguageCatalog &LanguageCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static LanguageCatalog language_catalog{txn};
  return language_catalog;
}

LanguageCatalog::~LanguageCatalog(){};

LanguageCatalog::LanguageCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." CATALOG_SCHEMA_NAME "." LANGUAGE_CATALOG_NAME
                      " ("
                      "language_oid   INT NOT NULL PRIMARY KEY, "
                      "lanname        VARCHAR NOT NULL);",
                      txn) {
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, CATALOG_SCHEMA_NAME, LANGUAGE_CATALOG_NAME, {1},
      LANGUAGE_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);
}

// insert a new language by name
bool LanguageCatalog::InsertLanguage(const std::string &lanname,
                                     type::AbstractPool *pool,
                                     concurrency::TransactionContext *txn) {
  (void) pool;
  std::vector<std::vector<ExpressionPtr>> tuples;
  tuples.push_back(std::vector<ExpressionPtr>());
  auto &values = tuples[0];

  oid_t language_oid = GetNextOid();
  auto val0 = type::ValueFactory::GetIntegerValue(language_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(lanname);

  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val0)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val1)));

  // Insert the tuple
  return InsertTupleWithCompiledPlan(&tuples, txn);
}

// delete a language by name
bool LanguageCatalog::DeleteLanguage(const std::string &lanname,
                                     concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids(all_column_ids);

  auto *lan_name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                                    ColumnId::LANNAME);
  lan_name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::LANNAME);
      
      expression::AbstractExpression *lan_name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(lanname, nullptr).Copy());
  expression::AbstractExpression *lan_name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, lan_name_expr,
          lan_name_const_expr);

  return DeleteWithCompiledSeqScan(column_ids, lan_name_equality_expr, txn);
}

std::unique_ptr<LanguageCatalogObject> LanguageCatalog::GetLanguageByOid(
    oid_t lang_oid, concurrency::TransactionContext *txn) const {

  std::vector<oid_t> column_ids(all_column_ids);

  auto *oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        ColumnId::OID);
  oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                        catalog_table_->GetOid(), ColumnId::OID);

  expression::AbstractExpression *oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(lang_oid).Copy());
  expression::AbstractExpression *oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, oid_expr, oid_const_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, oid_equality_expr, txn);


  PELOTON_ASSERT(result_tuples.size() <= 1);

  std::unique_ptr<LanguageCatalogObject> ret;
  if (!result_tuples.empty()) {
    ret.reset(new LanguageCatalogObject(std::move(result_tuples[0])));
  }

  return ret;
}

std::unique_ptr<LanguageCatalogObject> LanguageCatalog::GetLanguageByName(
    const std::string &lang_name, concurrency::TransactionContext *txn) const {

  std::vector<oid_t> column_ids(all_column_ids);

  auto *name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                                    ColumnId::LANNAME);
  name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                          catalog_table_->GetOid(), ColumnId::LANNAME);
  expression::AbstractExpression *name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(lang_name, nullptr).Copy());
  expression::AbstractExpression *name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, name_expr, name_const_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, name_equality_expr, txn);

  PELOTON_ASSERT(result_tuples.size() <= 1);

  std::unique_ptr<LanguageCatalogObject> ret;
  if (!result_tuples.empty()) {
    ret.reset(new LanguageCatalogObject(std::move(result_tuples[0])));
  }

  return ret;
}

}  // namespace catalog
}  // namespace peloton
