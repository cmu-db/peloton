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
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

namespace peloton {
namespace catalog {

/*@brief   insert table catalog object into cache
* @param   table_object
* @param   forced   if forced, replace existing object, else return false if oid
*          already exists
* @return  false only if not forced and table_name already exists in cache
*/
bool DatabaseCatalogObject::InsertTableObject(
    std::shared_ptr<TableCatalogObject> table_object, bool forced) {
  if (!table_object || table_object->table_oid == INVALID_OID) {
    return false;  // invalid object
  }

  // find old table catalog object
  // std::lock_guard<std::mutex> lock(table_cache_lock);
  auto it = table_name_cache.find(table_object->table_name);

  // force evict if found
  if (forced == true && it != table_name_cache.end()) {
    if (it->second == table_object->table_oid) {
      return false;  // no need to replace
    }
    EvictTableObject(table_object->table_name);  // evict old
    it = table_name_cache.find(table_object->table_name);
  }

  // insert into table name cache
  if (it == table_name_cache.end()) {
    table_name_cache.insert(
        std::make_pair(table_object->table_name, table_object->table_oid));
    txn->catalog_cache.InsertTableObject(table_object, forced);
    return true;
  } else {
    return false;  // table name already exists
  }
}

/*@brief   evict table catalog object from cache
* @param   table_name
* @return  true if table_oid is found and evicted; false if not found
*/
bool DatabaseCatalogObject::EvictTableObject(const std::string &table_name) {
  // std::lock_guard<std::mutex> lock(table_cache_lock);

  // find table name from table name cache
  auto it = table_name_cache.find(table_name);
  if (it != table_name_cache.end()) {
    oid_t table_oid = it->second;
    table_name_cache.erase(it);
    txn->catalog_cache.EvictTableObject(table_oid);
    return true;
  } else {
    return false;  // table name not found in cache
  }
}

/* @brief   Get table catalog object from cache or all the way from storage
 * @param   table_oid     table oid of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogObject> DatabaseCatalogObject::GetTableObject(
    oid_t table_oid, bool cached_only) {
  if (cached_only) {
    // return table object from cache
    return txn->catalog_cache.GetTableObject(table_oid);
  } else {
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
  if (cached_only) {
    // translate table name to table_oid from cache
    // std::lock_guard<std::mutex> lock(table_cache_lock);
    auto it = table_name_cache.find(table_name);
    if (it == table_name_cache.end()) {
      LOG_DEBUG("table %s not found in database %u", table_name.c_str(),
                database_oid);
      return nullptr;
    }
    oid_t table_oid = it->second;

    // return table object from cache
    return txn->catalog_cache.GetTableObject(table_oid);
  } else {
    return TableCatalog::GetInstance()->GetTableObject(table_name, database_oid,
                                                       txn);
  }
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

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::shared_ptr<DatabaseCatalogObject> DatabaseCatalog::GetDatabaseObject(
    oid_t database_oid, concurrency::Transaction *txn) {
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
    if (database_object) {
      // insert into cache
      bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
      if (success == false) {
        LOG_DEBUG("Database catalog for database %u is already in cache",
                  database_object->database_oid);
      }
    }
    return database_object;
  } else {
    LOG_DEBUG("Found %lu database tiles with oid %u", result_tiles->size(),
              database_oid);
  }

  return std::shared_ptr<DatabaseCatalogObject>();
}

std::shared_ptr<DatabaseCatalogObject> DatabaseCatalog::GetDatabaseObject(
    const std::string &database_name, concurrency::Transaction *txn) {
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
      if (success == false) {
        LOG_DEBUG("Database catalog for database %u is already in cache",
                  database_object->database_oid);
      }
    }
    return database_object;
  } else {
    LOG_DEBUG("Found %lu database tiles with name %s", result_tiles->size(),
              database_name.c_str());
  }

  return std::shared_ptr<DatabaseCatalogObject>();
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
