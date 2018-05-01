//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_catalog.cpp
//
// Identification: src/catalog/column_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/column_catalog.h"

#include "catalog/catalog.h"
#include "catalog/system_catalogs.h"
#include "catalog/table_catalog.h"
#include "codegen/buffering_consumer.h"
#include "concurrency/transaction_context.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "type/value_factory.h"
#include "expression/expression_util.h"

namespace peloton {
namespace catalog {

ColumnCatalogObject::ColumnCatalogObject(executor::LogicalTile *tile,
                                         int tupleId)
    : table_oid(tile->GetValue(tupleId, ColumnCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
      column_name(tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_NAME)
                      .ToString()),
      column_id(tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_ID)
                    .GetAs<uint32_t>()),
      column_offset(
          tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_OFFSET)
              .GetAs<uint32_t>()),
      column_type(StringToTypeId(
          tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_TYPE)
              .ToString())),
      is_inlined(tile->GetValue(tupleId, ColumnCatalog::ColumnId::IS_INLINED)
                     .GetAs<bool>()),
      is_primary(tile->GetValue(tupleId, ColumnCatalog::ColumnId::IS_PRIMARY)
                     .GetAs<bool>()),
      is_not_null(tile->GetValue(tupleId, ColumnCatalog::ColumnId::IS_NOT_NULL)
                      .GetAs<bool>()) {}

ColumnCatalogObject::ColumnCatalogObject(codegen::WrappedTuple wrapped_tuple)
    : table_oid(wrapped_tuple.GetValue(ColumnCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
      column_name(wrapped_tuple.GetValue(ColumnCatalog::ColumnId::COLUMN_NAME)
                      .ToString()),
      column_id(wrapped_tuple.GetValue(ColumnCatalog::ColumnId::COLUMN_ID)
                    .GetAs<oid_t>()),
      column_offset(
          wrapped_tuple.GetValue(ColumnCatalog::ColumnId::COLUMN_OFFSET)
              .GetAs<oid_t>()),
      column_type(StringToTypeId(
          wrapped_tuple.GetValue(ColumnCatalog::ColumnId::COLUMN_TYPE)
              .ToString())),
      is_inlined(wrapped_tuple.GetValue(ColumnCatalog::ColumnId::IS_INLINED)
                     .GetAs<bool>()),
      is_primary(wrapped_tuple.GetValue(ColumnCatalog::ColumnId::IS_PRIMARY)
                     .GetAs<bool>()),
      is_not_null(wrapped_tuple.GetValue(ColumnCatalog::ColumnId::IS_NOT_NULL)
                      .GetAs<bool>()) {}


ColumnCatalog::ColumnCatalog(storage::Database *pg_catalog,
                             type::AbstractPool *pool,
                             concurrency::TransactionContext *txn)
    : AbstractCatalog(COLUMN_CATALOG_OID, COLUMN_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Add indexes for pg_attribute
  AddIndex({ColumnId::TABLE_OID, ColumnId::COLUMN_NAME},
           COLUMN_CATALOG_PKEY_OID, COLUMN_CATALOG_NAME "_pkey",
           IndexConstraintType::PRIMARY_KEY);
  AddIndex({ColumnId::TABLE_OID, ColumnId::COLUMN_ID}, COLUMN_CATALOG_SKEY0_OID,
           COLUMN_CATALOG_NAME "_skey0", IndexConstraintType::UNIQUE);
  AddIndex({ColumnId::TABLE_OID}, COLUMN_CATALOG_SKEY1_OID,
           COLUMN_CATALOG_NAME "_skey1", IndexConstraintType::DEFAULT);

  // Insert columns of pg_attribute table into pg_attribute itself
  uint32_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    InsertColumn(COLUMN_CATALOG_OID, column.GetName(), column_id,
                 column.GetOffset(), column.GetType(), column.IsInlined(),
                 column.GetConstraints(), pool, txn);
    column_id++;
  }
}

ColumnCatalog::~ColumnCatalog() {}

/*@brief   private function for initialize schema of pg_attribute
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> ColumnCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "column_name", false);
  column_name_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  column_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "column_id", true);
  column_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_offset_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "column_offset", true);
  column_offset_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_type_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "column_type", false);
  column_type_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto is_inlined_column = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "is_inlined", true);
  is_inlined_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto is_primary_column = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "is_primary", true);
  is_primary_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto is_not_null_column = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "is_not_null", true);
  is_not_null_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> column_catalog_schema(new catalog::Schema(
      {table_id_column, column_name_column, column_id_column,
       column_offset_column, column_type_column, is_inlined_column,
       is_primary_column, is_not_null_column}));

  return column_catalog_schema;
}

bool ColumnCatalog::InsertColumn(oid_t table_oid,
                                 const std::string &column_name,
                                 oid_t column_id, oid_t column_offset,
                                 type::TypeId column_type, bool is_inlined,
                                 const std::vector<Constraint> &constraints,
                                 type::AbstractPool *pool,
                                 concurrency::TransactionContext *txn) {
  (void) pool;
  // Create the tuple first
  std::vector<std::vector<ExpressionPtr>> tuples;

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(column_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(column_id);
  auto val3 = type::ValueFactory::GetIntegerValue(column_offset);
  auto val4 =
      type::ValueFactory::GetVarcharValue(TypeIdToString(column_type), nullptr);
  auto val5 = type::ValueFactory::GetBooleanValue(is_inlined);
  bool is_primary = false, is_not_null = false;
  for (auto constraint : constraints) {
    if (constraint.GetType() == ConstraintType::PRIMARY) {
      is_primary = true;
    } else if (constraint.GetType() == ConstraintType::NOT_NULL ||
               constraint.GetType() == ConstraintType::NOTNULL) {
      is_not_null = true;
    }
  }
  auto val6 = type::ValueFactory::GetBooleanValue(is_primary);
  auto val7 = type::ValueFactory::GetBooleanValue(is_not_null);

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
  // Insert the tuple
  return InsertTupleWithCompiledPlan(&tuples, txn);
}

bool ColumnCatalog::DeleteColumn(oid_t table_oid,
                                 const std::string &column_name,
                                 concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids(all_column_ids);

  auto *table_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::TABLE_OID);
  table_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::TABLE_OID);

  expression::AbstractExpression *table_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_oid).Copy());
  expression::AbstractExpression *table_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, table_oid_expr, table_oid_const_expr);

  auto *col_name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                           ColumnId::COLUMN_NAME);
  col_name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                               catalog_table_->GetOid(),
                               ColumnId::COLUMN_NAME);
  expression::AbstractExpression *col_name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());
  expression::AbstractExpression *col_name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, col_name_expr,
          col_name_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, table_oid_equality_expr,
          col_name_equality_expr);

  // delete column from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  table_object->EvictColumnObject(column_name);

  return DeleteWithCompiledSeqScan(column_ids, predicate, txn);
}

/* @brief   delete all column records from the same table
 * this function is useful when calling DropTable
 * @param   table_oid
 * @param   txn  TransactionContext
 * @return  a vector of table oid
 */
bool ColumnCatalog::DeleteColumns(oid_t table_oid,
                                  concurrency::TransactionContext *txn) {

  std::vector<oid_t> column_ids(all_column_ids);

  auto *table_oid_expr =
    new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                         ColumnId::TABLE_OID);
  table_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::TABLE_OID);

  expression::AbstractExpression *table_oid_const_expr =
    expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(table_oid).Copy());
  expression::AbstractExpression *table_oid_equality_expr =
    expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, table_oid_expr, table_oid_const_expr);

  bool result =  DeleteWithCompiledSeqScan(column_ids, table_oid_equality_expr, txn);

  // delete columns from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
    table_object->EvictAllColumnObjects();
  return result;
}

const std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
ColumnCatalog::GetColumnObjects(oid_t table_oid,
                                concurrency::TransactionContext *txn) {
  // try get from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);
  auto column_objects = table_object->GetColumnObjects(true);
  if (column_objects.size() != 0) return column_objects;

  // cache miss, get from pg_attribute
  std::vector<oid_t> column_ids(all_column_ids);

  auto tb_oid_expr =
    new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                         ColumnId::TABLE_OID);

  tb_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::TABLE_OID);

  expression::AbstractExpression *tb_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_oid).Copy());
  expression::AbstractExpression *col_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, tb_oid_expr, tb_oid_const_expr);

  expression::AbstractExpression *predicate = col_oid_equality_expr;
  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  for (auto tuple : result_tuples) {
    auto column_object = std::make_shared<ColumnCatalogObject>(tuple);
    table_object->InsertColumnObject(column_object);
  }

  return table_object->GetColumnObjects();
}

}  // namespace catalog
}  // namespace peloton
