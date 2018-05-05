//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_catalog.cpp
//
// Identification: src/catalog/zone_map_catalog.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/zone_map_catalog.h"

#include "catalog/catalog.h"
#include "common/internal_types.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "type/value_factory.h"
#include "expression/expression_util.h"
#include "codegen/buffering_consumer.h"

namespace peloton {
namespace catalog {

// Global Singleton : I really dont want to do this and I know this sucks.
// but #796 is still not merged when I am writing this code and I really
// am sorry to do this. When PelotonMain() becomes a reality, I will
// fix this for sure.
ZoneMapCatalog *ZoneMapCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static ZoneMapCatalog zone_map_catalog{txn};
  return &zone_map_catalog;
}

ZoneMapCatalog::ZoneMapCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." CATALOG_SCHEMA_NAME "." ZONE_MAP_CATALOG_NAME
                      " ("
                      "database_id    INT NOT NULL, "
                      "table_id       INT NOT NULL, "
                      "tile_group_id  INT NOT NULL,  "
                      "column_id      INT NOT NULL, "
                      "minimum        VARCHAR, "
                      "maximum        VARCHAR, "
                      "type           VARCHAR);",
                      txn) {
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, CATALOG_SCHEMA_NAME, ZONE_MAP_CATALOG_NAME,
      {0, 1, 2, 3}, ZONE_MAP_CATALOG_NAME "_skey0", true, IndexType::BWTREE,
      txn);
}

ZoneMapCatalog::~ZoneMapCatalog() {}

bool ZoneMapCatalog::InsertColumnStatistics(
    oid_t database_id, oid_t table_id, oid_t tile_group_id, oid_t column_id,
    std::string minimum, std::string maximum, std::string type,
    type::AbstractPool *pool, concurrency::TransactionContext *txn) {
 (void) pool;
 // Create the tuple first
 std::vector<std::vector<ExpressionPtr>> tuples;

  auto val_db_id = type::ValueFactory::GetIntegerValue(database_id);
  auto val_table_id = type::ValueFactory::GetIntegerValue(table_id);
  auto val_tile_group_id = type::ValueFactory::GetIntegerValue(tile_group_id);
  auto val_column_id = type::ValueFactory::GetIntegerValue(column_id);
  auto val_minimum = type::ValueFactory::GetVarcharValue(minimum);
  auto val_maximum = type::ValueFactory::GetVarcharValue(maximum);
  auto val_type = type::ValueFactory::GetVarcharValue(type);

 auto constant_db_id_expr = new expression::ConstantValueExpression(
   val_db_id);
 auto constant_table_id_expr = new expression::ConstantValueExpression(
   val_table_id);
 auto constant_tile_group_id_expr = new expression::ConstantValueExpression(
   val_tile_group_id);
 auto constant_column_id_expr = new expression::ConstantValueExpression(
   val_column_id);
 auto constant_minimum_expr = new expression::ConstantValueExpression(
   val_minimum);
 auto constant_maximum_expr = new expression::ConstantValueExpression(
   val_maximum);
 auto constant_type_expr = new expression::ConstantValueExpression(
   val_type);

 tuples.push_back(std::vector<ExpressionPtr>());
 auto &values = tuples[0];

 values.push_back(ExpressionPtr(constant_db_id_expr));
 values.push_back(ExpressionPtr(constant_table_id_expr));
 values.push_back(ExpressionPtr(constant_tile_group_id_expr));
 values.push_back(ExpressionPtr(constant_column_id_expr));
 values.push_back(ExpressionPtr(constant_minimum_expr));
 values.push_back(ExpressionPtr(constant_maximum_expr));
 values.push_back(ExpressionPtr(constant_type_expr));

 return InsertTupleWithCompiledPlan(&tuples, txn);
}

bool ZoneMapCatalog::DeleteColumnStatistics(
    oid_t database_id, oid_t table_id, oid_t tile_group_id, oid_t column_id,
    concurrency::TransactionContext *txn) {
 std::vector<oid_t> column_ids(all_column_ids);

 auto db_oid_expr =
   new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        ColumnId::DATABASE_ID);
 db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::DATABASE_ID);
 expression::AbstractExpression *db_oid_const_expr =
   expression::ExpressionUtil::ConstantValueFactory(
     type::ValueFactory::GetIntegerValue(database_id).Copy());
 expression::AbstractExpression *db_oid_equality_expr =
   expression::ExpressionUtil::ComparisonFactory(
     ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);


 auto tb_oid_expr =
   new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        ColumnId::TABLE_ID);
 tb_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::TABLE_ID);
 expression::AbstractExpression *tb_oid_const_expr =
   expression::ExpressionUtil::ConstantValueFactory(
     type::ValueFactory::GetIntegerValue(table_id).Copy());
 expression::AbstractExpression *tb_oid_equality_expr =
   expression::ExpressionUtil::ComparisonFactory(
     ExpressionType::COMPARE_EQUAL, tb_oid_expr, tb_oid_const_expr);

 auto tile_gid_expr  =
   new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        ColumnId::TILE_GROUP_ID);
 tile_gid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::TILE_GROUP_ID);
 expression::AbstractExpression *tile_gid_const_expr =
   expression::ExpressionUtil::ConstantValueFactory(
     type::ValueFactory::GetIntegerValue(tile_group_id).Copy());
 expression::AbstractExpression *tile_gid_equality_expr =
   expression::ExpressionUtil::ComparisonFactory(
     ExpressionType::COMPARE_EQUAL, tile_gid_expr, tile_gid_const_expr);

 auto col_oid_expr  =
   new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        ColumnId::COLUMN_ID);
 col_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::COLUMN_ID);
 expression::AbstractExpression *col_oid_const_expr =
   expression::ExpressionUtil::ConstantValueFactory(
     type::ValueFactory::GetIntegerValue(column_id).Copy());
 expression::AbstractExpression *col_id_equality_expr =
   expression::ExpressionUtil::ComparisonFactory(
     ExpressionType::COMPARE_EQUAL, col_oid_expr, col_oid_const_expr);

 expression::AbstractExpression *db_and_tb =
   expression::ExpressionUtil::ConjunctionFactory(
     ExpressionType::CONJUNCTION_AND, db_oid_equality_expr,
     tb_oid_equality_expr);
 expression::AbstractExpression *pred_and_tile =
   expression::ExpressionUtil::ConjunctionFactory(
     ExpressionType::CONJUNCTION_AND, db_and_tb, tile_gid_equality_expr);
 expression::AbstractExpression *predicate =
   expression::ExpressionUtil::ConjunctionFactory(
     ExpressionType::CONJUNCTION_AND, pred_and_tile, col_id_equality_expr);

 return DeleteWithCompiledSeqScan(column_ids, predicate, txn);
}

std::unique_ptr<std::vector<type::Value>> ZoneMapCatalog::GetColumnStatistics(
    oid_t database_id, oid_t table_id, oid_t tile_group_id, oid_t column_id,
    concurrency::TransactionContext *txn) {

  std::vector<oid_t> column_ids(all_column_ids);

 auto db_oid_expr =
   new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        ColumnId::DATABASE_ID);
 db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::DATABASE_ID);
  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_id).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);

 auto tb_oid_expr =
   new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        ColumnId::TABLE_ID);
 tb_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::TABLE_ID);
  expression::AbstractExpression *tb_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_id).Copy());
  expression::AbstractExpression *tb_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, tb_oid_expr, tb_oid_const_expr);

 auto tile_gid_expr  =
   new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        ColumnId::TILE_GROUP_ID);
 tile_gid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::TILE_GROUP_ID);
  expression::AbstractExpression *tile_gid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(tile_group_id).Copy());
  expression::AbstractExpression *tile_gid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, tile_gid_expr, tile_gid_const_expr);

 auto col_oid_expr  =
   new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                        ColumnId::COLUMN_ID);
 col_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(), catalog_table_->GetOid(), ColumnId::COLUMN_ID);
 expression::AbstractExpression *col_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(column_id).Copy());
  expression::AbstractExpression *col_id_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, col_oid_expr, col_oid_const_expr);

  expression::AbstractExpression *db_and_tb =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, db_oid_equality_expr,
          tb_oid_equality_expr);
  expression::AbstractExpression *pred_and_tile =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, db_and_tb, tile_gid_equality_expr);
  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, pred_and_tile, col_id_equality_expr);

  auto result_tuples = GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  PELOTON_ASSERT(result_tuples.size() <= 1);  // unique
  if (result_tuples.empty()) {
    LOG_DEBUG("Result Tiles = 0");
    return nullptr;
  }
  auto tuple = result_tuples[0];
  type::Value min, max, actual_type;
  min = tuple.GetValue(ColumnId ::MINIMUM);
  max = tuple.GetValue(ColumnId::MAXIMUM);
  actual_type = tuple.GetValue(ColumnId::TYPE);

  // min and max are stored as VARCHARs and should be convertd to their
  // original types.

  std::unique_ptr<std::vector<type::Value>> zone_map(
      new std::vector<type::Value>({min, max, actual_type}));

  return zone_map;
}

}  // namespace catalog
}  // namespace peloton
