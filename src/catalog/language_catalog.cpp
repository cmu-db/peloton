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

#include <include/codegen/buffering_consumer.h>
#include <include/expression/expression_util.h>
#include "catalog/language_catalog.h"

#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

LanguageCatalogObject::LanguageCatalogObject(executor::LogicalTile *tuple)
    : lang_oid_(tuple->GetValue(0, 0).GetAs<oid_t>()),
      lang_name_(tuple->GetValue(0, 1).GetAs<const char *>()) {}

  LanguageCatalogObject::LanguageCatalogObject(codegen::WrappedTuple tuple)
      : lang_oid_(tuple.GetValue(0).GetAs<oid_t>()),
        lang_name_(tuple.GetValue(1).GetAs<const char *>()) {}

LanguageCatalog &LanguageCatalog::GetInstance(concurrency::TransactionContext *txn) {
  static LanguageCatalog language_catalog{txn};
  return language_catalog;
}

LanguageCatalog::~LanguageCatalog(){};

LanguageCatalog::LanguageCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." LANGUAGE_CATALOG_NAME
                      " ("
                      "language_oid   INT NOT NULL PRIMARY KEY, "
                      "lanname        VARCHAR NOT NULL);",
                      txn) {
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, LANGUAGE_CATALOG_NAME, {1},
      LANGUAGE_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);
}

// insert a new language by name
bool LanguageCatalog::InsertLanguage(const std::string &lanname,
                                     type::AbstractPool *pool,
                                     concurrency::TransactionContext *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  oid_t language_oid = GetNextOid();
  auto val0 = type::ValueFactory::GetIntegerValue(language_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(lanname);

  tuple->SetValue(ColumnId::OID, val0, pool);
  tuple->SetValue(ColumnId::LANNAME, val1, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

// delete a language by name
bool LanguageCatalog::DeleteLanguage(const std::string &lanname,
                                     concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::SECONDARY_KEY_0;

  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(lanname, nullptr).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::unique_ptr<LanguageCatalogObject> LanguageCatalog::GetLanguageByOid(
    oid_t lang_oid, concurrency::TransactionContext *txn) const {
  /*std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(lang_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  PL_ASSERT(result_tiles->size() <= 1);

  std::unique_ptr<LanguageCatalogObject> ret;
  if (result_tiles->size() == 1) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    ret.reset(new LanguageCatalogObject((*result_tiles)[0].get()));
  }*/

  std::vector<oid_t> column_ids(all_column_ids);

  expression::AbstractExpression *oid_expr = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, ColumnId::OID);
  expression::AbstractExpression *oid_const_expr = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(lang_oid).Copy());
  expression::AbstractExpression *oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, oid_expr, oid_const_expr);


  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, oid_equality_expr, txn);


  PL_ASSERT(result_tuples.size() <= 1);

  std::unique_ptr<LanguageCatalogObject> ret;
  if (result_tuples.size() == 1) {
    ret.reset(new LanguageCatalogObject(result_tuples[0]));
  }

  return ret;
}

std::unique_ptr<LanguageCatalogObject> LanguageCatalog::GetLanguageByName(
    const std::string &lang_name, concurrency::TransactionContext *txn) const {
  /*
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(lang_name).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  PL_ASSERT(result_tiles->size() <= 1);

  std::unique_ptr<LanguageCatalogObject> ret;
  if (result_tiles->size() == 1) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    ret.reset(new LanguageCatalogObject((*result_tiles)[0].get()));
  }*/

  std::vector<oid_t> column_ids(all_column_ids);

  expression::AbstractExpression *name_expr = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::VARCHAR, 0, ColumnId::LANNAME);
  expression::AbstractExpression *name_const_expr = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetVarcharValue(lang_name, nullptr).Copy());
  expression::AbstractExpression *name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, name_expr,
          name_const_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, name_equality_expr, txn);

  PL_ASSERT(result_tuples.size() <= 1);

  std::unique_ptr<LanguageCatalogObject> ret;
  if (result_tuples.size() == 1) {
    ret.reset(new LanguageCatalogObject(result_tuples[0]));
  }

  return ret;
}

}  // namespace catalog
}  // namespace peloton
