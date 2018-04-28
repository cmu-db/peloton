//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// proc_catalog.cpp
//
// Identification: src/catalog/proc_catalog.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/proc_catalog.h"

#include "catalog/catalog.h"
#include "catalog/language_catalog.h"
#include "codegen/buffering_consumer.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"
#include "expression/expression_util.h"

namespace peloton {
namespace catalog {

#define PROC_CATALOG_NAME "pg_proc"

ProcCatalogObject::ProcCatalogObject(executor::LogicalTile *tile,
                                     concurrency::TransactionContext *txn)
    : oid_(tile->GetValue(0, 0).GetAs<oid_t>()),
      name_(tile->GetValue(0, 1).GetAs<const char *>()),
      ret_type_(tile->GetValue(0, 2).GetAs<type::TypeId>()),
      arg_types_(StringToTypeArray(tile->GetValue(0, 3).GetAs<const char *>())),
      lang_oid_(tile->GetValue(0, 4).GetAs<oid_t>()),
      src_(tile->GetValue(0, 5).GetAs<const char *>()),
      txn_(txn) {}

ProcCatalogObject::ProcCatalogObject(codegen::WrappedTuple wrapped_tuple,
                                     concurrency::TransactionContext *txn)
    : oid_(wrapped_tuple.GetValue(0).GetAs<oid_t>()),
      name_(wrapped_tuple.GetValue(1).GetAs<const char *>()),
      ret_type_(wrapped_tuple.GetValue(2).GetAs<type::TypeId>()),
      arg_types_(
          StringToTypeArray(wrapped_tuple.GetValue(3).GetAs<const char *>())),
      lang_oid_(wrapped_tuple.GetValue(4).GetAs<oid_t>()),
      src_(wrapped_tuple.GetValue(5).GetAs<const char *>()),
      txn_(txn) {}

std::unique_ptr<LanguageCatalogObject> ProcCatalogObject::GetLanguage() const {
  return LanguageCatalog::GetInstance().GetLanguageByOid(GetLangOid(), txn_);
}

ProcCatalog &ProcCatalog::GetInstance(concurrency::TransactionContext *txn) {
  static ProcCatalog proc_catalog{txn};
  return proc_catalog;
}

ProcCatalog::~ProcCatalog(){};

ProcCatalog::ProcCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." PROC_CATALOG_NAME
                      " ("
                      "proc_oid      INT NOT NULL PRIMARY KEY, "
                      "proname       VARCHAR NOT NULL, "
                      "prorettype    INT NOT NULL, "
                      "proargtypes   VARCHAR NOT NULL, "
                      "prolang       INT NOT NULL, "
                      "prosrc        VARCHAR NOT NULL);",
                      txn) {
  Catalog::GetInstance()->CreateIndex(CATALOG_DATABASE_NAME, PROC_CATALOG_NAME,
                                      {1, 3}, PROC_CATALOG_NAME "_skey0", false,
                                      IndexType::BWTREE, txn);
}

bool ProcCatalog::InsertProc(const std::string &proname,
                             type::TypeId prorettype,
                             const std::vector<type::TypeId> &proargtypes,
                             oid_t prolang, const std::string &prosrc,
                             type::AbstractPool *pool,
                             concurrency::TransactionContext *txn) {
  (void) pool;
  // Create the tuple first
  std::vector<std::vector<ExpressionPtr>> tuples;

  oid_t proc_oid = GetNextOid();
  auto val0 = type::ValueFactory::GetIntegerValue(proc_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(proname);
  auto val2 = type::ValueFactory::GetIntegerValue(static_cast<int>(prorettype));
  auto val3 =
      type::ValueFactory::GetVarcharValue(TypeIdArrayToString(proargtypes));
  auto val4 = type::ValueFactory::GetIntegerValue(prolang);
  auto val5 = type::ValueFactory::GetVarcharValue(prosrc);

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

  tuples.push_back(std::vector<ExpressionPtr>());
  auto &values = tuples[0];
  values.push_back(ExpressionPtr(constant_expr_0));
  values.push_back(ExpressionPtr(constant_expr_1));
  values.push_back(ExpressionPtr(constant_expr_2));
  values.push_back(ExpressionPtr(constant_expr_3));
  values.push_back(ExpressionPtr(constant_expr_4));
  values.push_back(ExpressionPtr(constant_expr_5));

  return InsertTupleWithCompiledPlan(&tuples, txn);
}

std::unique_ptr<ProcCatalogObject> ProcCatalog::GetProcByOid(
    oid_t proc_oid, concurrency::TransactionContext *txn) const {
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(proc_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  PELOTON_ASSERT(result_tiles->size() <= 1);

  std::unique_ptr<ProcCatalogObject> ret;
  if (result_tiles->size() == 1) {
    PELOTON_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    ret.reset(new ProcCatalogObject((*result_tiles)[0].get(), txn));
  }

  return ret;
}

std::unique_ptr<ProcCatalogObject> ProcCatalog::GetProcByName(
    const std::string &proc_name,
    const std::vector<type::TypeId> &proc_arg_types,
    concurrency::TransactionContext *txn) const {
  std::vector<oid_t> column_ids(all_column_ids);

  auto *proc_name_expr =
    new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                                    ColumnId::PRONAME);
  proc_name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                              catalog_table_->GetOid(), ColumnId::PRONAME);

  expression::AbstractExpression *proc_name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(proc_name, nullptr).Copy());
  expression::AbstractExpression *proc_name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, proc_name_expr, proc_name_const_expr);

  auto *proc_args_expr =
    new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                                    ColumnId::PROARGTYPES);
  proc_args_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                              catalog_table_->GetOid(), ColumnId::PROARGTYPES);
  expression::AbstractExpression *proc_args_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(
              TypeIdArrayToString(proc_arg_types)).Copy());
  expression::AbstractExpression *proc_args_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, proc_args_expr, proc_args_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, proc_name_equality_expr,
          proc_args_equality_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  PELOTON_ASSERT(result_tuples.size() <= 1);

  std::unique_ptr<ProcCatalogObject> ret;
  if (result_tuples.size() == 1) {
    ret.reset(new ProcCatalogObject(result_tuples[0], txn));
  }

  return ret;
}

}  // namespace catalog
}  // namespace peloton
