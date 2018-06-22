//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_cache.h
//
// Identification: src/include/catalog/catalog_cache.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <vector>
#include <unordered_map>

#include "common/internal_types.h"

namespace peloton {

namespace planner {
class PlanUtil;
}  // namespace planner

namespace function {
class SequenceFunctions;
}  // namespace function

namespace catalog {

class DatabaseCatalogObject;
class SequenceCatalogObject;
class TableCatalogObject;
class IndexCatalogObject;

class CatalogCache {
  friend class DatabaseCatalog;
  friend class TableCatalog;
  friend class IndexCatalog;
  friend class DatabaseCatalogObject;
  friend class TableCatalogObject;
  friend class IndexCatalogObject;
  friend class planner::PlanUtil;
  friend class SequenceCatalogObject;
  friend class function::SequenceFunctions;

 public:
  CatalogCache() {}
  CatalogCache(CatalogCache const &) = delete;
  CatalogCache &operator=(CatalogCache const &) = delete;

 private:

  /**
   * @brief   get database catalog object from cache
   * @param   database_oid
   * @return  database catalog object; if not found return object with invalid oid
   */
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(oid_t database_oid);

  /**
   * @brief   get database catalog object from cache
   * @param   database_name
   * @return  database catalog object; if not found return null
   */
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      const std::string &name);

  /**
   * @brief Retrieve a vector of all the DatabaseObjects cached
   * @return
   */
  std::vector<std::shared_ptr<DatabaseCatalogObject>> GetAllDatabaseObjects();

  /**
   * @brief   search table catalog object from all cached database objects
   * @param   table_oid
   * @return  table catalog object; if not found return null
   */
  std::shared_ptr<TableCatalogObject> GetCachedTableObject(oid_t database_oid, oid_t table_oid);

  /**
   * @brief   search index catalog object from all cached database objects
   * @param   index_oid
   * @return  index catalog object; if not found return null
   */
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(oid_t database_oid, oid_t index_oid);

  /**
   * @brief   search index catalog object from all cached database objects
   * @param   index_name
   * @return  index catalog object; if not found return null
   */
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(
      const std::string &database_name, const std::string &index_name,
      const std::string &schema_name);

  // database catalog cache interface
  bool InsertDatabaseObject(
      std::shared_ptr<DatabaseCatalogObject> database_object);
  bool EvictDatabaseObject(oid_t database_oid);
  bool EvictDatabaseObject(const std::string &database_name);

  /**
   * @brief   insert sequence catalog object into cache
   * @param  sequence_object
   * @return  false only if sequence already exists in cache or invalid
   */
  bool InsertSequenceObject(
          std::shared_ptr<SequenceCatalogObject> sequence_object);

  /**
   * @brief   evict sequence catalog object from cache
   * @param  sequence_name
   * @param  database_oid
   * @return  true if specified sequence is found and evicted;
   *          false if not found
   */
  bool EvictSequenceObject(const std::string &sequence_name,
          oid_t database_oid);

  /**
   * @brief Get sequence catalog object from cache
   * @param database_oid
   * @param namespace_oid
   * @param sequence_name
   * @return sequence catalog object; if not found return object with invalid oid
   */
  std::shared_ptr<SequenceCatalogObject> GetSequenceObject(
          oid_t database_oid,
          oid_t namespace_oid,
          const std::string &sequence_name);

  // cache for database catalog object
  std::unordered_map<oid_t, std::shared_ptr<DatabaseCatalogObject>>
      database_objects_cache;
  std::unordered_map<std::string, std::shared_ptr<DatabaseCatalogObject>>
      database_name_cache;

  // cache for sequence catalog object
//  std::unordered_map<std::pair<oid_t, std::string>,
//                     std::shared_ptr<SequenceCatalogObject>>
//      sequence_objects_cache;

};

}  // namespace catalog
}  // namespace peloton
