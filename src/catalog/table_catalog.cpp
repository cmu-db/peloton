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

#include <memory>

#include "catalog/table_catalog.h"

#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/column_catalog.h"
#include "concurrency/transaction.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

namespace peloton {
namespace catalog {

/* @brief   insert index catalog object into cache
 * @param   index_object
 * @return  false if index_name already exists in cache
 */
bool TableCatalogObject::InsertIndexObject(
    std::shared_ptr<IndexCatalogObject> index_object) {
  if (!index_object || index_object->index_oid == INVALID_OID) {
    return false;  // invalid object
  }
  // std::lock_guard<std::mutex> lock(index_cache_lock);

  // check if already in cache
  if (index_objects.find(index_object->index_oid) != index_objects.end()) {
    LOG_DEBUG("Index %u already exists in cache!", index_object->index_oid);
    return false;
  }

  if (index_names.find(index_object->index_name) != index_names.end()) {
    LOG_DEBUG("Index %s already exists in cache!",
              index_object->index_name.c_str());
    return false;
  }

  valid_index_objects = true;
  index_objects.insert(std::make_pair(index_object->index_oid, index_object));
  index_names.insert(std::make_pair(index_object->index_name, index_object));
  return true;
}

/* @brief   evict index catalog object from cache
 * @param   index_oid
 * @return  true if index_oid is found and evicted; false if not found
 */
bool TableCatalogObject::EvictIndexObject(oid_t index_oid) {
  // std::lock_guard<std::mutex> lock(index_cache_lock);
  if (!valid_index_objects) return false;

  // find index name from index name cache
  auto it = index_objects.find(index_oid);
  if (it == index_objects.end()) {
    return false;  // index oid not found in cache
  }

  auto index_object = it->second;
  PL_ASSERT(index_object);
  index_objects.erase(it);
  index_names.erase(index_object->index_name);
  return true;
}

/* @brief   evict index catalog object from cache
 * @param   index_name
 * @return  true if index_name is found and evicted; false if not found
 */
bool TableCatalogObject::EvictIndexObject(const std::string &index_name) {
  // std::lock_guard<std::mutex> lock(index_cache_lock);
  if (!valid_index_objects) return false;

  // find index name from index name cache
  auto it = index_names.find(index_name);
  if (it == index_names.end()) {
    return false;  // index name not found in cache
  }

  auto index_object = it->second;
  PL_ASSERT(index_object);
  index_names.erase(it);
  index_objects.erase(index_object->index_oid);
  return true;
}

/* @brief   evict all index catalog objects from cache
 */
void TableCatalogObject::EvictAllIndexObjects() {
  // std::lock_guard<std::mutex> lock(index_cache_lock);
  index_objects.clear();
  index_names.clear();
  valid_index_objects = false;
}

/* @brief   get all index objects of this table into cache
 * @return  map from index oid to cached index object
 */
std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>>
TableCatalogObject::GetIndexObjects(bool cached_only) {
  // std::lock_guard<std::mutex> lock(index_cache_lock);
  if (!valid_index_objects && !cached_only) {
    // get index catalog objects from pg_index
    valid_index_objects = true;
    index_objects =
        IndexCatalog::GetInstance()->GetIndexObjects(table_oid, txn);
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
  // std::lock_guard<std::mutex> lock(index_cache_lock);
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
  // std::lock_guard<std::mutex> lock(index_cache_lock);
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
  if (!column_object || column_object->table_oid == INVALID_OID) {
    return false;  // invalid object
  }
  // std::lock_guard<std::mutex> lock(column_cache_lock);

  // check if already in cache
  if (column_objects.find(column_object->column_id) != column_objects.end()) {
    LOG_DEBUG("Column %u already exists in cache!", column_object->column_id);
    return false;
  }

  if (column_names.find(column_object->column_name) != column_names.end()) {
    LOG_DEBUG("Column %s already exists in cache!",
              column_object->column_name.c_str());
    return false;
  }

  valid_column_objects = true;
  column_objects.insert(
      std::make_pair(column_object->column_id, column_object));
  column_names.insert(
      std::make_pair(column_object->column_name, column_object));
  return true;
}

/* @brief   evict column catalog object from cache
 * @param   column_id
 * @return  true if column_id is found and evicted; false if not found
 */
bool TableCatalogObject::EvictColumnObject(oid_t column_id) {
  // std::lock_guard<std::mutex> lock(column_cache_lock);
  if (!valid_column_objects) return false;

  // find column name from column name cache
  auto it = column_objects.find(column_id);
  if (it == column_objects.end()) {
    return false;  // column id not found in cache
  }

  auto column_object = it->second;
  PL_ASSERT(column_object);
  column_objects.erase(it);
  column_names.erase(column_object->column_name);
  return true;
}

/* @brief   evict column catalog object from cache
 * @param   column_name
 * @return  true if column_name is found and evicted; false if not found
 */
bool TableCatalogObject::EvictColumnObject(const std::string &column_name) {
  // std::lock_guard<std::mutex> lock(column_cache_lock);
  if (!valid_column_objects) return false;

  // find column name from column name cache
  auto it = column_names.find(column_name);
  if (it == column_names.end()) {
    return false;  // column name not found in cache
  }

  auto column_object = it->second;
  PL_ASSERT(column_object);
  column_names.erase(it);
  column_objects.erase(column_object->column_id);
  return true;
}

/* @brief   evict all column catalog objects from cache
 * @return  true if column_name is found and evicted; false if not found
 */
void TableCatalogObject::EvictAllColumnObjects() {
  // std::lock_guard<std::mutex> lock(column_cache_lock);
  column_objects.clear();
  column_names.clear();
  valid_column_objects = false;
}

/* @brief   get all column objects of this table into cache
 * @return  map from column id to cached column object
 */
std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
TableCatalogObject::GetColumnObjects(bool cached_only) {
  // std::lock_guard<std::mutex> lock(column_cache_lock);
  if (!valid_column_objects && !cached_only) {
    // get column catalog objects from pg_column
    ColumnCatalog::GetInstance()->GetColumnObjects(table_oid, txn);
    valid_column_objects = true;
  }
  return column_objects;
}

/* @brief   get column object with column id from cache
 * @param   column_id
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached column object, nullptr if not found
 */
std::shared_ptr<ColumnCatalogObject> TableCatalogObject::GetColumnObject(
    oid_t column_id, bool cached_only) {
  // std::lock_guard<std::mutex> lock(column_cache_lock);
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
  // std::lock_guard<std::mutex> lock(column_cache_lock);
  GetColumnObjects(cached_only);  // fetch column objects in case we have not
  auto it = column_names.find(column_name);
  if (it != column_names.end()) {
    return it->second;
  }
  return nullptr;
}

TableCatalog *TableCatalog::GetInstance(storage::Database *pg_catalog,
                                        type::AbstractPool *pool,
                                        concurrency::Transaction *txn) {
  static TableCatalog table_catalog{pg_catalog, pool, txn};
  return &table_catalog;
}

TableCatalog::TableCatalog(storage::Database *pg_catalog,
                           type::AbstractPool *pool,
                           concurrency::Transaction *txn)
    : AbstractCatalog(TABLE_CATALOG_OID, TABLE_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Insert columns into pg_attribute
  ColumnCatalog *pg_attribute =
      ColumnCatalog::GetInstance(pg_catalog, pool, txn);

  oid_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    pg_attribute->InsertColumn(TABLE_CATALOG_OID, column.GetName(), column_id,
                               column.GetOffset(), column.GetType(),
                               column.IsInlined(), column.GetConstraints(),
                               pool, txn);
    column_id++;
  }
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

  auto database_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "database_oid", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> table_catalog_schema(new catalog::Schema(
      {table_id_column, table_name_column, database_id_column}));

  return table_catalog_schema;
}

/*@brief   insert a tuple about table info into pg_table
 * @param   table_oid
 * @param   table_name
 * @param   database_oid
 * @param   txn     Transaction
 * @return  Whether insertion is Successful
 */
bool TableCatalog::InsertTable(oid_t table_oid, const std::string &table_name,
                               oid_t database_oid, type::AbstractPool *pool,
                               concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(database_oid);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);
  tuple->SetValue(2, val2, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

/*@brief   delete a tuple about table info from pg_table(using index scan)
 * @param   table_oid
 * @param   txn     Transaction
 * @return  Whether deletion is Successful
 */
bool TableCatalog::DeleteTable(oid_t table_oid, concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  // evict from cache
  auto table_object = txn->catalog_cache.GetCachedTableObject(table_oid);
  if (table_object) {
    auto database_object = DatabaseCatalog::GetInstance()->GetDatabaseObject(
        table_object->database_oid, txn);
    database_object->EvictTableObject(table_oid);
  }

  return DeleteWithIndexScan(index_offset, values, txn);
}

/*@brief   read table catalog object from pg_table using table oid
 * @param   table_oid
 * @param   txn     Transaction
 * @return  table catalog object
 */
std::shared_ptr<TableCatalogObject> TableCatalog::GetTableObject(
    oid_t table_oid, concurrency::Transaction *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto table_object = txn->catalog_cache.GetCachedTableObject(table_oid);
  if (table_object) return table_object;

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids({0, 1, 2});
  oid_t index_offset = 0;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto table_object =
        std::make_shared<TableCatalogObject>((*result_tiles)[0].get(), txn);
    // insert into cache
    auto database_object = DatabaseCatalog::GetInstance()->GetDatabaseObject(
        table_object->database_oid, txn);
    PL_ASSERT(database_object);
    bool success = database_object->InsertTableObject(table_object);
    PL_ASSERT(success == true);
    (void)success;
    return table_object;
  } else {
    LOG_DEBUG("Found %lu table with oid %u", result_tiles->size(), table_oid);
  }

  return nullptr;
}

/*@brief   read table catalog object from pg_table using table name + database
 * oid
 * @param   table_name
 * @param   database_oid
 * @param   txn     Transaction
 * @return  table catalog object
 */
std::shared_ptr<TableCatalogObject> TableCatalog::GetTableObject(
    const std::string &table_name, oid_t database_oid,
    concurrency::Transaction *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid);
  if (database_object) {
    auto table_object = database_object->GetTableObject(table_name, true);
    if (table_object) return table_object;
  }

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids({0, 1, 2});
  oid_t index_offset = 1;  // Index of table_name & database_oid
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(table_name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto table_object =
        std::make_shared<TableCatalogObject>((*result_tiles)[0].get(), txn);
    // insert into cache
    auto database_object = DatabaseCatalog::GetInstance()->GetDatabaseObject(
        table_object->database_oid, txn);
    PL_ASSERT(database_object);
    bool success = database_object->InsertTableObject(table_object);
    PL_ASSERT(success == true);
    (void)success;
    return table_object;
  } else {
    // LOG_DEBUG("Found %lu table with name %s", result_tiles->size(),
    //           table_name.c_str());
  }
  return nullptr;
}

// /*@brief   read table name from pg_table using table oid
//  * @param   table_oid
//  * @param   txn     Transaction
//  * @return  table name
//  */
// std::string TableCatalog::GetTableName(oid_t table_oid,
//                                        concurrency::Transaction *txn) {
//   auto table_object = GetTableObject(table_oid, txn);
//   if (table_object) {
//     return table_object->table_name;
//   } else {
//     return std::string();
//   }

//   // std::vector<oid_t> column_ids({1});  // table_name
//   // oid_t index_offset = 0;              // Index of table_oid
//   // std::vector<type::Value> values;
//   // values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

//   // auto result_tiles =
//   //     GetResultWithIndexScan(column_ids, index_offset, values, txn);

//   // std::string table_name;
//   // PL_ASSERT(result_tiles->size() <= 1);  // table_oid is unique
//   // if (result_tiles->size() != 0) {
//   //   PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
//   //   if ((*result_tiles)[0]->GetTupleCount() != 0) {
//   //     table_name = (*result_tiles)[0]
//   //                      ->GetValue(0, 0)
//   //                      .ToString();  // After projection left 1 column
//   //   }
//   // }

//   // return table_name;
// }

// /*@brief   read database oid one table belongs to using table oid
//  * @param   table_oid
//  * @param   txn     Transaction
//  * @return  database oid
//  */
// oid_t TableCatalog::GetDatabaseOid(oid_t table_oid,
//                                    concurrency::Transaction *txn) {
//   auto table_object = GetTableObject(table_oid, txn);
//   if (table_object) {
//     return table_object->database_oid;
//   } else {
//     return INVALID_OID;
//   }

//   // std::vector<oid_t> column_ids({2});  // database_oid
//   // oid_t index_offset = 0;              // Index of table_oid
//   // std::vector<type::Value> values;
//   // values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

//   // auto result_tiles =
//   //     GetResultWithIndexScan(column_ids, index_offset, values, txn);

//   // oid_t database_oid = INVALID_OID;
//   // PL_ASSERT(result_tiles->size() <= 1);  // table_oid is unique
//   // if (result_tiles->size() != 0) {
//   //   PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
//   //   if ((*result_tiles)[0]->GetTupleCount() != 0) {
//   //     database_oid = (*result_tiles)[0]
//   //                        ->GetValue(0, 0)
//   //                        .GetAs<oid_t>();  // After projection left 1
//   column
//   //   }
//   // }

//   // return database_oid;
// }

// /*@brief   read table oid from pg_table using table name + database oid
//  * @param   table_name
//  * @param   database_oid
//  * @param   txn     Transaction
//  * @return  table oid
//  */
// oid_t TableCatalog::GetTableOid(const std::string &table_name,
//                                 oid_t database_oid,
//                                 concurrency::Transaction *txn) {
//   auto table_object = GetTableObject(table_name, database_oid, txn);
//   if (table_object) {
//     return table_object->table_oid;
//   } else {
//     return INVALID_OID;
//   }

//   // std::vector<oid_t> column_ids({0});  // table_oid
//   // oid_t index_offset = 1;              // Index of table_name &
//   database_oid
//   // std::vector<type::Value> values;
//   // values.push_back(
//   //     type::ValueFactory::GetVarcharValue(table_name, nullptr).Copy());
//   //
//   values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

//   // auto result_tiles =
//   //     GetResultWithIndexScan(column_ids, index_offset, values, txn);

//   // oid_t table_oid = INVALID_OID;
//   // PL_ASSERT(result_tiles->size() <= 1);  // table_name & database_oid is
//   // unique
//   // if (result_tiles->size() != 0) {
//   //   PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
//   //   if ((*result_tiles)[0]->GetTupleCount() != 0) {
//   //     table_oid = (*result_tiles)[0]
//   //                     ->GetValue(0, 0)
//   //                     .GetAs<oid_t>();  // After projection left 1 column
//   //   }
//   // }

//   // return table_oid;
// }

// /*@brief   read all table oids from the same database using its oid
//  * @param   database_oid
//  * @param   txn  Transaction
//  * @return  a vector of table oid
//  */
// std::vector<oid_t> TableCatalog::GetTableOids(oid_t database_oid,
//                                               concurrency::Transaction *txn)
//                                               {
//   std::vector<oid_t> column_ids({0});  // table_oid
//   oid_t index_offset = 2;              // Index of database_oid
//   std::vector<type::Value> values;
//   values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

//   auto result_tiles =
//       GetResultWithIndexScan(column_ids, index_offset, values, txn);

//   std::vector<oid_t> table_ids;
//   for (auto &tile : (*result_tiles)) {
//     for (auto tuple_id : *tile) {
//       table_ids.emplace_back(
//           tile->GetValue(tuple_id, 0)
//               .GetAs<oid_t>());  // After projection left 1 column
//     }
//   }

//   return table_ids;
// }

// /*@brief   read all table names from the same database using its oid
//  * @param   database_oid
//  * @param   txn  Transaction
//  * @return  a vector of table name
//  */
// std::vector<std::string> TableCatalog::GetTableNames(
//     oid_t database_oid, concurrency::Transaction *txn) {
//   std::vector<oid_t> column_ids({1});  // table_name
//   oid_t index_offset = 2;              // Index of database_oid
//   std::vector<type::Value> values;
//   values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

//   auto result_tiles =
//       GetResultWithIndexScan(column_ids, index_offset, values, txn);

//   std::vector<std::string> table_names;
//   for (auto &tile : (*result_tiles)) {
//     for (auto tuple_id : *tile) {
//       table_names.emplace_back(
//           tile->GetValue(tuple_id, 0)
//               .ToString());  // After projection left 1 column
//     }
//   }

//   return table_names;
// }

}  // namespace catalog
}  // namespace peloton
