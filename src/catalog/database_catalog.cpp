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

#include <memory>

#include "catalog/database_catalog.h"

#include "concurrency/transaction.h"
#include "catalog/column_catalog.h"
#include "concurrency/transaction.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

namespace peloton {
namespace catalog {

/* @brief   insert table catalog object into cache
 * @param   table_object
 * @return  false if table_name already exists in cache
 */
bool DatabaseCatalogObject::InsertTableObject(
    std::shared_ptr<TableCatalogObject> table_object) {
  if (!table_object || table_object->table_oid == INVALID_OID) {
    return false;  // invalid object
  }
  // std::lock_guard<std::mutex> lock(table_cache_lock);

  // check if already in cache
  if (table_objects_cache.find(table_object->table_oid) !=
      table_objects_cache.end()) {
    LOG_DEBUG("Table %u already exists in cache!", table_object->table_oid);
    return false;
  }

  if (table_name_cache.find(table_object->table_name) !=
      table_name_cache.end()) {
    LOG_DEBUG("Table %s already exists in cache!",
              table_object->table_name.c_str());
    return false;
  }

  table_objects_cache.insert(
      std::make_pair(table_object->table_oid, table_object));
  table_name_cache.insert(
      std::make_pair(table_object->table_name, table_object));
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_oid
 * @return  true if table_oid is found and evicted; false if not found
 */
bool DatabaseCatalogObject::EvictTableObject(oid_t table_oid) {
  // std::lock_guard<std::mutex> lock(table_cache_lock);

  // find table name from table name cache
  auto it = table_objects_cache.find(table_oid);
  if (it == table_objects_cache.end()) {
    return false;  // table oid not found in cache
  }

  auto table_object = it->second;
  PL_ASSERT(table_object);
  table_objects_cache.erase(it);
  table_name_cache.erase(table_object->table_name);
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_name
 * @return  true if table_name is found and evicted; false if not found
 */
bool DatabaseCatalogObject::EvictTableObject(const std::string &table_name) {
  // std::lock_guard<std::mutex> lock(table_cache_lock);

  // find table name from table name cache
  auto it = table_name_cache.find(table_name);
  if (it == table_name_cache.end()) {
    return false;  // table oid not found in cache
  }

  auto table_object = it->second;
  PL_ASSERT(table_object);
  table_name_cache.erase(it);
  table_objects_cache.erase(table_object->table_oid);
  return true;
}

/*@brief   evict all table catalog objects in this database from cache
*/
void DatabaseCatalogObject::EvictAllTableObjects() {
  // std::lock_guard<std::mutex> lock(table_cache_lock);
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
    return TableCatalog::GetInstance()->GetTableObject(table_oid, txn);
  }
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   table_oid     table oid of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogObject> DatabaseCatalogObject::GetTableObject(
    const std::string &table_name, bool cached_only) {
  auto it = table_name_cache.find(table_name);
  if (it != table_name_cache.end()) return it->second;

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    return TableCatalog::GetInstance()->GetTableObject(table_name, database_oid,
                                                       txn);
  }
}

/*@brief   search index catalog object from all cached database objects
* @param   index_oid
* @return  index catalog object; if not found return null
*/
std::shared_ptr<IndexCatalogObject> DatabaseCatalogObject::GetCachedIndexObject(
    oid_t index_oid) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
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
    const std::string &index_name) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  for (auto it = table_objects_cache.begin(); it != table_objects_cache.end();
       ++it) {
    auto table_object = it->second;
    auto index_object = table_object->GetIndexObject(index_name, true);
    if (index_object) return index_object;
  }
  return nullptr;
}

DatabaseCatalog *DatabaseCatalog::GetInstance(storage::Database *pg_catalog,
                                              type::AbstractPool *pool,
                                              concurrency::Transaction *txn) {
  static DatabaseCatalog database_catalog{pg_catalog, pool, txn};
  return &database_catalog;
}

DatabaseCatalog::DatabaseCatalog(storage::Database *pg_catalog,
                                 type::AbstractPool *pool,
                                 concurrency::Transaction *txn)
    : AbstractCatalog(DATABASE_CATALOG_OID, DATABASE_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Insert columns into pg_attribute
  ColumnCatalog *pg_attribute =
      ColumnCatalog::GetInstance(pg_catalog, pool, txn);

  oid_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    pg_attribute->InsertColumn(DATABASE_CATALOG_OID, column.GetName(),
                               column_id, column.GetOffset(), column.GetType(),
                               column.IsInlined(), column.GetConstraints(),
                               pool, txn);
    column_id++;
  }
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
                                     concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool DatabaseCatalog::DeleteDatabase(oid_t database_oid,
                                     concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  // evict cache
  txn->catalog_cache.EvictDatabaseObject(database_oid);

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::shared_ptr<DatabaseCatalogObject> DatabaseCatalog::GetDatabaseObject(
    oid_t database_oid, concurrency::Transaction *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid);
  if (database_object) return database_object;

  // cache miss, get from pg_database
  std::vector<oid_t> column_ids({0, 1});
  oid_t index_offset = 0;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogObject>((*result_tiles)[0].get(), txn);
    // insert into cache
    bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
    PL_ASSERT(success == true);
    return database_object;
  } else {
    LOG_DEBUG("Found %lu database tiles with oid %u", result_tiles->size(),
              database_oid);
  }
  return nullptr;
}

std::shared_ptr<DatabaseCatalogObject> DatabaseCatalog::GetDatabaseObject(
    const std::string &database_name, concurrency::Transaction *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_name);
  if (database_object) return database_object;

  // cache miss, get from pg_database
  std::vector<oid_t> column_ids({0, 1});
  oid_t index_offset = 1;  // Index of database_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogObject>((*result_tiles)[0].get(), txn);
    if (database_object) {
      // insert into cache
      bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
      PL_ASSERT(success == true);
    }
    return database_object;
  } else {
    // LOG_DEBUG("Found %lu database tiles with name %s", result_tiles->size(),
    //           database_name.c_str());
  }
  return nullptr;
}

std::string DatabaseCatalog::GetDatabaseName(oid_t database_oid,
                                             concurrency::Transaction *txn) {
  auto database_object = GetDatabaseObject(database_oid, txn);
  if (database_object) {
    return database_object->database_name;
  } else {
    return std::string();
  }

  // std::vector<oid_t> column_ids({1});  // database_name
  // oid_t index_offset = 0;              // Index of database_oid
  // std::vector<type::Value> values;
  // values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  // auto result_tiles =
  //     GetResultWithIndexScan(column_ids, index_offset, values, txn);

  // std::string database_name;
  // PL_ASSERT(result_tiles->size() <= 1);  // database_oid is unique
  // if (result_tiles->size() != 0) {
  //   PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
  //   if ((*result_tiles)[0]->GetTupleCount() != 0) {
  //     database_name = (*result_tiles)[0]
  //                         ->GetValue(0, 0)
  //                         .ToString();  // After projection left 1 column
  //   }
  // }

  // return database_name;
}

oid_t DatabaseCatalog::GetDatabaseOid(const std::string &database_name,
                                      concurrency::Transaction *txn) {
  auto database_object = GetDatabaseObject(database_name, txn);
  if (database_object) {
    return database_object->database_oid;
  } else {
    return INVALID_OID;
  }

  // std::vector<oid_t> column_ids({0});  // database_oid
  // oid_t index_offset = 1;              // Index of database_name
  // std::vector<type::Value> values;
  // values.push_back(
  //     type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  // auto result_tiles =
  //     GetResultWithIndexScan(column_ids, index_offset, values, txn);

  // oid_t database_oid = INVALID_OID;
  // PL_ASSERT(result_tiles->size() <= 1);  // database_name is unique
  // if (result_tiles->size() != 0) {
  //   PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
  //   if ((*result_tiles)[0]->GetTupleCount() != 0) {
  //     database_oid = (*result_tiles)[0]
  //                        ->GetValue(0, 0)
  //                        .GetAs<oid_t>();  // After projection left 1 column
  //   }
  // }

  // return database_oid;
}

}  // namespace catalog
}  // namespace peloton
