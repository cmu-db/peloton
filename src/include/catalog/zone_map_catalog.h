//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_catalog.h
//
// Identification: src/include/catalog/zone_map_catalog.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <map>

#include "catalog/abstract_catalog.h"

#define ZONE_MAP_CATALOG_NAME "zone_map"

namespace peloton {
namespace catalog {

class ZoneMapCatalog : public AbstractCatalog {
 public:
  ~ZoneMapCatalog();

  // Global Singleton : I really dont want to do this and I know this sucks.
  // but #796 is still not merged when I am writing this code and I really
  // am sorry to do this. When PelotonMain() becomes a reality, I will
  // fix this for sure.

  static ZoneMapCatalog *GetInstance(concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertColumnStatistics(oid_t database_id, oid_t table_id,
                              oid_t tile_group_id, oid_t column_id,
                              std::string minimum, std::string maximum,
                              std::string type, type::AbstractPool *pool,
                              concurrency::TransactionContext *txn);

  bool DeleteColumnStatistics(oid_t database_id, oid_t table_id,
                              oid_t tile_group_id, oid_t column_id,
                              concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//

  std::unique_ptr<std::vector<type::Value>> GetColumnStatistics(
      oid_t database_id, oid_t table_id, oid_t tile_group_id, oid_t column_id,
      concurrency::TransactionContext *txn);

  enum class ColumnId {
    DATABASE_ID = 0,
    TABLE_ID = 1,
    TILE_GROUP_ID = 2,
    COLUMN_ID = 3,
    MINIMUM = 4,
    MAXIMUM = 5,
    TYPE = 6
  };

  enum class ZoneMapOffset { 
    MINIMUM_OFF = 0, 
    MAXIMUM_OFF = 1, 
    TYPE_OFF = 2 
  };

 private:
  ZoneMapCatalog(concurrency::TransactionContext *txn);

  enum class IndexId {
    SECONDARY_KEY_0 = 0,
  };
};

}  // namespace catalog
}  // namespace peloton