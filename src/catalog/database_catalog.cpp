//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_catalog.cpp
//
// Identification: src/catalog/database_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/database_catalog.h"

#include <memory>

#include "catalog/catalog.h"
#include "catalog/system_catalogs.h"
#include "concurrency/transaction_context.h"
#include "expression/expression_util.h"
#include "codegen/buffering_consumer.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

DatabaseCatalogObject::DatabaseCatalogObject(
    codegen::WrappedTuple wrapped_tuple, concurrency::TransactionContext *txn)
    : database_oid(
          wrapped_tuple.GetValue(DatabaseCatalog::ColumnId::DATABASE_OID)
              .GetAs<oid_t>()),
      database_name(
          wrapped_tuple.GetValue(DatabaseCatalog::ColumnId::DATABASE_NAME)
              .ToString()),
      table_objects_cache(),
      table_name_cache(),
      valid_table_objects(false),
      txn(txn) {}

/* @brief   insert table catalog object into cache
 * @param   table_object
 * @return  false if table_name already exists in cache
 */
bool DatabaseCatalogObject::InsertTableObject(
    std::shared_ptr<TableCatalogObject> table_object) {
  if (!table_object || table_object->GetTableOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (table_objects_cache.find(table_object->GetTableOid()) !=
      table_objects_cache.end()) {
    LOG_TRACE("Table %u already exists in cache!", table_object->GetTableOid());
    return false;
  }

  std::string key =
      table_object->GetSchemaName() + "." + table_object->GetTableName();
  if (table_name_cache.find(key) != table_name_cache.end()) {
    LOG_TRACE("Table %s already exists in cache!",
              table_object->GetTableName().c_str());
    return false;
  }

  table_objects_cache.insert(
      std::make_pair(table_object->GetTableOid(), table_object));
  table_name_cache.insert(std::make_pair(key, table_object));
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_oid
 * @return  true if table_oid is found and evicted; false if not found
 */
bool DatabaseCatalogObject::EvictTableObject(oid_t table_oid) {
  // find table name from table name cache
  auto it = table_objects_cache.find(table_oid);
  if (it == table_objects_cache.end()) {
    return false;  // table oid not found in cache
  }

  auto table_object = it->second;
  PELOTON_ASSERT(table_object);
  table_objects_cache.erase(it);
  // erase from table name cache
  std::string key =
      table_object->GetSchemaName() + "." + table_object->GetTableName();
  table_name_cache.erase(key);
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_name
 * @return  true if table_name is found and evicted; false if not found
 */
bool DatabaseCatalogObject::EvictTableObject(const std::string &table_name,
                                             const std::string &schema_name) {
  std::string key = schema_name + "." + table_name;
  // find table name from table name cache
  auto it = table_name_cache.find(key);
  if (it == table_name_cache.end()) {
    return false;  // table name not found in cache
  }

  auto table_object = it->second;
  PELOTON_ASSERT(table_object);
  table_name_cache.erase(it);
  table_objects_cache.erase(table_object->GetTableOid());
  return true;
}

/*@brief   evict all table catalog objects in this database from cache
 */
void DatabaseCatalogObject::EvictAllTableObjects() {
  table_objects_cache.clear();
  table_name_cache.clear();
}

/* @brief   Get table catalog object from cache or all the way from storage
 * @param   table_oid     table oid of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogObject> DatabaseCatalogObject::GetTableObject(
    oid_t table_oid, bool cached_only) {
  auto it = table_objects_cache.find(table_oid);
  if (it != table_objects_cache.end()) return it->second;

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid)
                        ->GetTableCatalog();
    return pg_table->GetTableObject(table_oid, txn);
  }
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   table_name     table name of the requested table catalog object
 * @param   schema_name    schema name of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogObject> DatabaseCatalogObject::GetTableObject(
    const std::string &table_name, const std::string &schema_name,
    bool cached_only) {
  std::string key = schema_name + "." + table_name;
  auto it = table_name_cache.find(key);
  if (it != table_name_cache.end()) {
    return it->second;
  }

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid)
                        ->GetTableCatalog();
    return pg_table->GetTableObject(table_name, schema_name, txn);
  }
}

/*@brief    Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   schema_name
 * @return  table catalog objects
 */
std::vector<std::shared_ptr<TableCatalogObject>>
DatabaseCatalogObject::GetTableObjects(const std::string &schema_name) {
  // read directly from pg_table
  if (!valid_table_objects) {
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid)
                        ->GetTableCatalog();
    // insert every table object into cache
    pg_table->GetTableObjects(txn);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PELOTON_ASSERT(valid_table_objects);
  std::vector<std::shared_ptr<TableCatalogObject>> result;
  for (auto it : table_objects_cache) {
    if (it.second->GetSchemaName() == schema_name) {
      result.push_back(it.second);
    }
  }

  return result;
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
DatabaseCatalogObject::GetTableObjects(bool cached_only) {
  if (!cached_only && !valid_table_objects) {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid)
                        ->GetTableCatalog();
    return pg_table->GetTableObjects(txn);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PELOTON_ASSERT(valid_table_objects);
  return table_objects_cache;
}

/*@brief   search index catalog object from all cached database objects
 * @param   index_oid
 * @return  index catalog object; if not found return null
 */
std::shared_ptr<IndexCatalogObject> DatabaseCatalogObject::GetCachedIndexObject(
    oid_t index_oid) {
  for (auto it = table_objects_cache.begin(); it != table_objects_cache.end();
       ++it) {
    auto table_object = it->second;
    auto index_object = table_object->GetIndexObject(index_oid, true);
    if (index_object) return index_object;
  }
  return nullptr;
}

/*@brief   search index catalog object from all cached database objects
 * @param   index_name
 * @return  index catalog object; if not found return null
 */
std::shared_ptr<IndexCatalogObject> DatabaseCatalogObject::GetCachedIndexObject(
    const std::string &index_name, const std::string &schema_name) {
  for (auto it = table_objects_cache.begin(); it != table_objects_cache.end();
       ++it) {
    auto table_object = it->second;
    if (table_object != nullptr &&
        table_object->GetSchemaName() == schema_name) {
      auto index_object = table_object->GetIndexObject(index_name, true);
      if (index_object) return index_object;
    }
  }
  return nullptr;
}

DatabaseCatalog *DatabaseCatalog::GetInstance(
    storage::Database *pg_catalog, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
  static DatabaseCatalog database_catalog{pg_catalog, pool, txn};
  return &database_catalog;
}

DatabaseCatalog::DatabaseCatalog(
    storage::Database *pg_catalog, UNUSED_ATTRIBUTE type::AbstractPool *pool,
    UNUSED_ATTRIBUTE concurrency::TransactionContext *txn)
    : AbstractCatalog(DATABASE_CATALOG_OID, DATABASE_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Add indexes for pg_database
  AddIndex({ColumnId::DATABASE_OID}, DATABASE_CATALOG_PKEY_OID,
           DATABASE_CATALOG_NAME "_pkey", IndexConstraintType::PRIMARY_KEY);
  AddIndex({ColumnId::DATABASE_NAME}, DATABASE_CATALOG_SKEY0_OID,
           DATABASE_CATALOG_NAME "_skey0", IndexConstraintType::UNIQUE);
}

DatabaseCatalog::~DatabaseCatalog() {}

/*@brief   private function for initialize schema of pg_database
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> DatabaseCatalog::InitializeSchema() {
  const std::string not_null_constraint_name = "not_null";
  const std::string primary_key_constraint_name = "primary_key";

  auto database_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "database_oid", true);
  database_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "database_name", false);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_catalog_schema(
      new catalog::Schema({database_id_column, database_name_column}));
  return database_catalog_schema;
}

bool DatabaseCatalog::InsertDatabase(oid_t database_oid,
                                     const std::string &database_name,
                                     type::AbstractPool *pool,
                                     concurrency::TransactionContext *txn) {

  (void) pool;

  std::vector<std::vector<ExpressionPtr>> tuples;
  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  auto constant_expr_0 = new expression::ConstantValueExpression(
      val0);
  auto constant_expr_1 = new expression::ConstantValueExpression(
      val1);

  tuples.emplace_back();
  auto &values = tuples[0];
  values.emplace_back(ExpressionPtr(constant_expr_0));
  values.emplace_back(ExpressionPtr(constant_expr_1));

  return InsertTupleWithCompiledPlan(&tuples, txn);
}

bool DatabaseCatalog::DeleteDatabase(oid_t database_oid,
                                     concurrency::TransactionContext *txn) {

  // cache miss, get from pg_database
  std::vector<oid_t> column_ids(all_column_ids);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::DATABASE_OID);

  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::DATABASE_OID);

  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_oid).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);

  expression::AbstractExpression *predicate = db_oid_equality_expr;

  // evict cache
  txn->catalog_cache.EvictDatabaseObject(database_oid);

  return DeleteWithCompiledSeqScan(column_ids, predicate, txn);
}

std::shared_ptr<DatabaseCatalogObject> DatabaseCatalog::GetDatabaseObject(
    oid_t database_oid, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid);
  if (database_object) return database_object;

  // cache miss, get from pg_database
  std::vector<oid_t> column_ids(all_column_ids);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                       ColumnId::DATABASE_OID);

  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::DATABASE_OID);

  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_oid).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);

  expression::AbstractExpression *predicate = db_oid_equality_expr;
  auto result_tuples = GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  if (result_tuples.size() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogObject>(result_tuples[0], txn);
    // insert into cache
    bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
    PELOTON_ASSERT(success == true);
    (void)success;
    return database_object;
  } else {
    LOG_TRACE("Found %lu database tiles with oid %u", result_tuples.size(),
              database_oid);
  }

  // return empty object if not found
  return nullptr;
}

/* @brief   First try get cached object from transaction cache. If cache miss,
 *          construct database object from pg_database, and insert into the
 *          cache.
 */
std::shared_ptr<DatabaseCatalogObject> DatabaseCatalog::GetDatabaseObject(
    const std::string &database_name, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_name);
  if (database_object) return database_object;

  std::vector<oid_t> column_ids(all_column_ids);

  auto *db_name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                       ColumnId::DATABASE_NAME);
  db_name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                              catalog_table_->GetOid(),
                              ColumnId::DATABASE_NAME);

    expression::AbstractExpression *db_name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());
  expression::AbstractExpression *db_name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_name_expr, db_name_const_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, db_name_equality_expr, txn);

  if (result_tuples.size() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogObject>(result_tuples[0], txn);
    // insert into cache
    bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
    PELOTON_ASSERT(success == true);
    (void)success;
    return database_object;
  }

    // return empty object if not found
  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
