//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_catalog.cpp
//
// Identification: src/catalog/table_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/table_catalog.h"

#include <memory>

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/system_catalogs.h"
#include "concurrency/transaction_context.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "codegen/buffering_consumer.h"
#include "type/value_factory.h"

#include "common/internal_types.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"

namespace peloton {
namespace catalog {

TableCatalogObject::TableCatalogObject(codegen::WrappedTuple wrapped_tuple,
                                       concurrency::TransactionContext *txn)
    : table_oid(wrapped_tuple.GetValue(TableCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
      table_name(wrapped_tuple.GetValue(TableCatalog::ColumnId::TABLE_NAME)
                     .ToString()),
      schema_name(wrapped_tuple.GetValue(TableCatalog::ColumnId::SCHEMA_NAME)
                      .ToString()),
      database_oid(wrapped_tuple.GetValue(TableCatalog::ColumnId::DATABASE_OID)
                       .GetAs<oid_t>()),
      version_id(wrapped_tuple.GetValue(TableCatalog::ColumnId::VERSION_ID)
                     .GetAs<uint32_t>()),
      index_objects(),
      index_names(),
      valid_index_objects(false),
      column_objects(),
      column_names(),
      valid_column_objects(false),
      txn(txn) {}

/* @brief   insert index catalog object into cache
 * @param   index_object
 * @return  false if index_name already exists in cache
 */
bool TableCatalogObject::InsertIndexObject(
    std::shared_ptr<IndexCatalogObject> index_object) {
  if (!index_object || index_object->GetIndexOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (index_objects.find(index_object->GetIndexOid()) != index_objects.end()) {
    LOG_DEBUG("Index %u already exists in cache!", index_object->GetIndexOid());
    return false;
  }

  if (index_names.find(index_object->GetIndexName()) != index_names.end()) {
    LOG_DEBUG("Index %s already exists in cache!",
              index_object->GetIndexName().c_str());
    return false;
  }

  valid_index_objects = true;
  index_objects.insert(
      std::make_pair(index_object->GetIndexOid(), index_object));
  index_names.insert(
      std::make_pair(index_object->GetIndexName(), index_object));
  return true;
}

/* @brief   evict index catalog object from cache
 * @param   index_oid
 * @return  true if index_oid is found and evicted; false if not found
 */
bool TableCatalogObject::EvictIndexObject(oid_t index_oid) {
  if (!valid_index_objects) return false;

  // find index name from index name cache
  auto it = index_objects.find(index_oid);
  if (it == index_objects.end()) {
    return false;  // index oid not found in cache
  }

  auto index_object = it->second;
  PELOTON_ASSERT(index_object);
  index_objects.erase(it);
  index_names.erase(index_object->GetIndexName());
  return true;
}

/* @brief   evict index catalog object from cache
 * @param   index_name
 * @return  true if index_name is found and evicted; false if not found
 */
bool TableCatalogObject::EvictIndexObject(const std::string &index_name) {
  if (!valid_index_objects) return false;

  // find index name from index name cache
  auto it = index_names.find(index_name);
  if (it == index_names.end()) {
    return false;  // index name not found in cache
  }

  auto index_object = it->second;
  PELOTON_ASSERT(index_object);
  index_names.erase(it);
  index_objects.erase(index_object->GetIndexOid());
  return true;
}

/* @brief   evict all index catalog objects from cache
 */
void TableCatalogObject::EvictAllIndexObjects() {
  index_objects.clear();
  index_names.clear();
  valid_index_objects = false;
}

/* @brief   get all index objects of this table into cache
 * @return  map from index oid to cached index object
 */
std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>>
TableCatalogObject::GetIndexObjects(bool cached_only) {
  if (!valid_index_objects && !cached_only) {
    // get index catalog objects from pg_index
    valid_index_objects = true;
    auto pg_index = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid)
                        ->GetIndexCatalog();
    index_objects = pg_index->GetIndexObjects(table_oid, txn);
  }
  return index_objects;
}

/* @brief   get index object with index oid from cache
 * @param   index_oid
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached index object, nullptr if not found
 */
std::shared_ptr<IndexCatalogObject> TableCatalogObject::GetIndexObject(
    oid_t index_oid, bool cached_only) {
  GetIndexObjects(cached_only);  // fetch index objects in case we have not
  auto it = index_objects.find(index_oid);
  if (it != index_objects.end()) {
    return it->second;
  }
  return nullptr;
}

/* @brief   get index object with index oid from cache
 * @param   index_name
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached index object, nullptr if not found
 */
std::shared_ptr<IndexCatalogObject> TableCatalogObject::GetIndexObject(
    const std::string &index_name, bool cached_only) {
  GetIndexObjects(cached_only);  // fetch index objects in case we have not
  auto it = index_names.find(index_name);
  if (it != index_names.end()) {
    return it->second;
  }
  return nullptr;
}

/* @brief   insert column catalog object into cache
 * @param   column_object
 * @return  false if column_name already exists in cache
 */
bool TableCatalogObject::InsertColumnObject(
    std::shared_ptr<ColumnCatalogObject> column_object) {
  if (!column_object || column_object->GetTableOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (column_objects.find(column_object->GetColumnId()) !=
      column_objects.end()) {
    LOG_DEBUG("Column %u already exists in cache!",
              column_object->GetColumnId());
    return false;
  }

  if (column_names.find(column_object->GetColumnName()) != column_names.end()) {
    LOG_DEBUG("Column %s already exists in cache!",
              column_object->GetColumnName().c_str());
    return false;
  }

  valid_column_objects = true;
  column_objects.insert(
      std::make_pair(column_object->GetColumnId(), column_object));
  column_names.insert(
      std::make_pair(column_object->GetColumnName(), column_object));
  return true;
}

/* @brief   evict column catalog object from cache
 * @param   column_id
 * @return  true if column_id is found and evicted; false if not found
 */
bool TableCatalogObject::EvictColumnObject(oid_t column_id) {
  if (!valid_column_objects) return false;

  // find column name from column name cache
  auto it = column_objects.find(column_id);
  if (it == column_objects.end()) {
    return false;  // column id not found in cache
  }

  auto column_object = it->second;
  PELOTON_ASSERT(column_object);
  column_objects.erase(it);
  column_names.erase(column_object->GetColumnName());
  return true;
}

/* @brief   evict column catalog object from cache
 * @param   column_name
 * @return  true if column_name is found and evicted; false if not found
 */
bool TableCatalogObject::EvictColumnObject(const std::string &column_name) {
  if (!valid_column_objects) return false;

  // find column name from column name cache
  auto it = column_names.find(column_name);
  if (it == column_names.end()) {
    return false;  // column name not found in cache
  }

  auto column_object = it->second;
  PELOTON_ASSERT(column_object);
  column_names.erase(it);
  column_objects.erase(column_object->GetColumnId());
  return true;
}

/* @brief   evict all column catalog objects from cache
 * @return  true if column_name is found and evicted; false if not found
 */
void TableCatalogObject::EvictAllColumnObjects() {
  column_objects.clear();
  column_names.clear();
  valid_column_objects = false;
}

/* @brief   get all column objects of this table into cache
 * @return  map from column id to cached column object
 */
std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
TableCatalogObject::GetColumnObjects(bool cached_only) {
  if (!valid_column_objects && !cached_only) {
    // get column catalog objects from pg_column
    auto pg_attribute = Catalog::GetInstance()
                            ->GetSystemCatalogs(database_oid)
                            ->GetColumnCatalog();
    pg_attribute->GetColumnObjects(table_oid, txn);
    valid_column_objects = true;
  }
  return column_objects;
}

/* @brief   get all column objects of this table into cache
 * @return  map from column name to cached column object
 */
std::unordered_map<std::string, std::shared_ptr<ColumnCatalogObject>>
TableCatalogObject::GetColumnNames(bool cached_only) {
  if (!valid_column_objects && !cached_only) {
    auto column_objects = GetColumnObjects();
    std::unordered_map<std::string, std::shared_ptr<ColumnCatalogObject>>
        column_names;
    for (auto &pair : column_objects) {
      auto column = pair.second;
      column_names[column->GetColumnName()] = column;
    }
  }
  return column_names;
}

/* @brief   get column object with column id from cache
 * @param   column_id
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached column object, nullptr if not found
 */
std::shared_ptr<ColumnCatalogObject> TableCatalogObject::GetColumnObject(
    oid_t column_id, bool cached_only) {
  GetColumnObjects(cached_only);  // fetch column objects in case we have not
  auto it = column_objects.find(column_id);
  if (it != column_objects.end()) {
    return it->second;
  }
  return nullptr;
}

/* @brief   get column object with column id from cache
 * @param   column_name
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached column object, nullptr if not found
 */
std::shared_ptr<ColumnCatalogObject> TableCatalogObject::GetColumnObject(
    const std::string &column_name, bool cached_only) {
  GetColumnObjects(cached_only);  // fetch column objects in case we have not
  auto it = column_names.find(column_name);
  if (it != column_names.end()) {
    return it->second;
  }
  return nullptr;
}

TableCatalog::TableCatalog(
    storage::Database *database, UNUSED_ATTRIBUTE type::AbstractPool *pool,
    UNUSED_ATTRIBUTE concurrency::TransactionContext *txn)
    : AbstractCatalog(TABLE_CATALOG_OID, TABLE_CATALOG_NAME,
                      InitializeSchema().release(), database) {
  // Add indexes for pg_namespace
  AddIndex({0}, TABLE_CATALOG_PKEY_OID, TABLE_CATALOG_NAME "_pkey",
           IndexConstraintType::PRIMARY_KEY);
  AddIndex({1, 2}, TABLE_CATALOG_SKEY0_OID, TABLE_CATALOG_NAME "_skey0",
           IndexConstraintType::UNIQUE);
  AddIndex({3}, TABLE_CATALOG_SKEY1_OID, TABLE_CATALOG_NAME "_skey1",
           IndexConstraintType::DEFAULT);
}

TableCatalog::~TableCatalog() {}

/*@brief   private function for initialize schema of pg_table
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> TableCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_name_column = catalog::Column(type::TypeId::VARCHAR, max_name_size,
                                           "table_name", false);
  table_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto schema_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "schema_name", false);
  schema_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "database_oid", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto version_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "version_id", true);
  version_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> table_catalog_schema(new catalog::Schema(
      {table_id_column, table_name_column, schema_name_column,
       database_id_column, version_id_column}));

  return table_catalog_schema;
}

/*@brief   insert a tuple about table info into pg_table
 * @param   table_oid
 * @param   table_name
 * @param   database_oid
 * @param   txn     TransactionContext
 * @return  Whether insertion is Successful
 */
bool TableCatalog::InsertTable(oid_t table_oid, const std::string &table_name,
                               const std::string &schema_name,
                               oid_t database_oid, type::AbstractPool *pool,
                               concurrency::TransactionContext *txn) {
  (void) pool;
  // Create the tuple first
  std::vector<std::vector<ExpressionPtr>> tuples;

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val2 = type::ValueFactory::GetVarcharValue(schema_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val4 = type::ValueFactory::GetIntegerValue(0);

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


//  tuples.push_back(std::vector<ExpressionPtr>());
  tuples.emplace_back();
  auto &values = tuples[0];
  values.emplace_back(constant_expr_0);
  values.emplace_back(constant_expr_1);
  values.emplace_back(constant_expr_2);
  values.emplace_back(constant_expr_3);
  values.emplace_back(constant_expr_4);

  return InsertTupleWithCompiledPlan(&tuples, txn);
}

/*@brief   delete a tuple about table info from pg_table(using index scan)
 * @param   table_oid
 * @param   txn     TransactionContext
 * @return  Whether deletion is successful
 */
bool TableCatalog::DeleteTable(oid_t table_oid,
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

  if(result) {
    // evict from cache
    auto table_object = txn->catalog_cache.GetCachedTableObject(table_oid);
    if (table_object) {
    auto database_object =
        DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);
      database_object->EvictTableObject(table_oid);
    }
  }
  return result;
}

/*@brief   read table catalog object from pg_table using table oid
 * @param   table_oid
 * @param   txn     TransactionContext
 * @return  table catalog object
 */
std::shared_ptr<TableCatalogObject> TableCatalog::GetTableObject(
    oid_t table_oid, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto table_object = txn->catalog_cache.GetCachedTableObject(table_oid);
  if (table_object) return table_object;

  // cache miss, get from pg_table
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

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, table_oid_equality_expr, txn);

  if (result_tuples.size() == 1) {
    auto table_object =
        std::make_shared<TableCatalogObject>(result_tuples[0], txn);

    // insert into cache
    auto database_object =
        DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);
    PELOTON_ASSERT(database_object);
    bool success = database_object->InsertTableObject(table_object);
    PELOTON_ASSERT(success == true);
    (void)success;

    return table_object;
  } else {
    LOG_DEBUG("Found %lu table with oid %u", result_tuples.size(), table_oid);
  }

  // return empty object if not found
  return nullptr;
}

/*@brief   read table catalog object from pg_table using table name + database
 * oid
 * @param   table_name
 * @param   schema_name
 * @param   database_oid
 * @param   txn     TransactionContext
 * @return  table catalog object
 */
std::shared_ptr<TableCatalogObject> TableCatalog::GetTableObject(
    const std::string &table_name, const std::string &schema_name,
    concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }

  //try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid);
  if (database_object) {
    auto table_object =
        database_object->GetTableObject(table_name, schema_name, true);
    if (table_object) return table_object;
  }

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids(all_column_ids);

  auto *table_name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                                    ColumnId::TABLE_NAME);
  table_name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                               catalog_table_->GetOid(),
                               ColumnId::TABLE_NAME);
  expression::AbstractExpression *table_name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(table_name, nullptr).Copy());
  expression::AbstractExpression *table_name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, table_name_expr,
          table_name_const_expr);

  auto *schema_name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                                    ColumnId::SCHEMA_NAME);
  schema_name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::SCHEMA_NAME);
  expression::AbstractExpression *schema_name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(schema_name, nullptr).Copy());
  expression::AbstractExpression *schema_name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, schema_name_expr, schema_name_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, table_name_equality_expr,
          schema_name_equality_expr);

  // ceate predicate refering to seq_scan_test.cpp
  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  if (result_tuples.size() == 1) {
    auto table_object =
        std::make_shared<TableCatalogObject>(result_tuples[0], txn);

    // insert into cache
    auto database_object =
        DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);
    PELOTON_ASSERT(database_object);
    bool success = database_object->InsertTableObject(table_object);
    PELOTON_ASSERT(success == true);
    (void)success;

    return table_object;
  }
  // return empty object if not found
  return nullptr;
}

/*@brief   read table catalog objects from pg_table using database oid
 * @param   database_oid
 * @param   txn     TransactionContext
 * @return  table catalog objects
 */
std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
TableCatalog::GetTableObjects(concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);
  PELOTON_ASSERT(database_object != nullptr);
  if (database_object->IsValidTableObjects()) {
    return database_object->GetTableObjects(true);
  }

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids(all_column_ids);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::DATABASE_OID);
  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(), ColumnId::DATABASE_OID);

  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_oid).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);

  expression::AbstractExpression *predicate = db_oid_equality_expr;
  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  for (auto &tuple : result_tuples) {
    auto table_object = std::make_shared<TableCatalogObject>(tuple, txn);
    database_object->InsertTableObject(table_object);
  }

  database_object->SetValidTableObjects(true);
  return database_object->GetTableObjects();
}

/*@brief    update version id column within pg_table
 * @param   update_val   the new(updated) version id
 * @param   table_oid    which table to be updated
 * @param   txn          TransactionContext
 * @return  Whether update is successful
 */
bool TableCatalog::UpdateVersionId(oid_t update_val, oid_t table_oid,
                                   concurrency::TransactionContext *txn) {
  std::vector<oid_t> update_columns({ColumnId::VERSION_ID});  // version_id
  // values to update
  std::vector<type::Value> update_values;
  update_values.push_back(
    type::ValueFactory::GetIntegerValue(update_val).Copy());
  // cache miss, get from pg_table
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

  // get table object, then evict table object
  auto table_object = txn->catalog_cache.GetCachedTableObject(table_oid);
  if (table_object) {
    auto database_object =
        DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);
    database_object->EvictTableObject(table_oid);
  }

  return UpdateWithCompiledSeqScan(update_columns, update_values, column_ids,
                                   table_oid_equality_expr, txn);
}

}  // namespace catalog
}  // namespace peloton
