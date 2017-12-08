//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_catalog.h
//
// Identification: src/include/catalog/zone_map_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
//
// Schema: (column offset: column_name)
// 0: database_id (pkey)
// 1: table_id (pkey)
// 2: tile_group_id (pkey)
// 3: column_id (pkey)
// 4: minimum
// 5: maximum
// 6: type
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

  static ZoneMapCatalog *GetInstance(concurrency::Transaction *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertColumnStatistics(oid_t database_id, oid_t table_id,
                              oid_t tile_group_id, oid_t column_id,
                              std::string minimum, std::string maximum,
                              std::string type, type::AbstractPool *pool,
                              concurrency::Transaction *txn);

  bool DeleteColumnStatistics(oid_t database_id, oid_t table_id,
                              oid_t tile_group_id, oid_t column_id,
                              concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//

  std::unique_ptr<std::vector<type::Value>> GetColumnStatistics(
      oid_t database_id, oid_t table_id, oid_t tile_group_id, oid_t column_id,
      concurrency::Transaction *txn);

  enum ColumnId {
    DATABASE_ID = 0,
    TABLE_ID = 1,
    TILE_GROUP_ID = 2,
    COLUMN_ID = 3,
    MINIMUM = 4,
    MAXIMUM = 5,
    TYPE = 6
  };

  enum ZoneMapOffset { MINIMUM_OFF = 0, MAXIMUM_OFF = 1, TYPE_OFF = 2 };

 private:
  ZoneMapCatalog(concurrency::Transaction *txn);

  enum IndexId {
    SECONDARY_KEY_0 = 0,
  };
};

}  // namespace catalog
}  // namespace peloton