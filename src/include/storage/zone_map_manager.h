//===----------------------------------------------------------------------===//
//
//                         Peloton
//
//
//
// Identification :
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sstream>

#include "common/macros.h"
#include "type/types.h"
#include "type/value_factory.h"
#include "concurrency/transaction.h"
namespace peloton {
namespace storage {

class DataTable;
class TileGroup;

struct PredicateInfo {
  int col_id;
  int comparison_operator;
  type::Value predicate_value;
};

class ZoneMapManager {
 public:
  typedef struct ColumnStatistics {
    type::Value min;
    type::Value max;
  } ColumnStatistics;

  // Global Singleton

  static ZoneMapManager *GetInstance();

  ZoneMapManager();

  void CreateZoneMapTableInCatalog();

  void CreateZoneMapsForTable(storage::DataTable *table,
                              concurrency::Transaction *txn);

  void CreateOrUpdateZoneMapForTileGroup(storage::DataTable *table, 
                                         oid_t tile_group_idx,
                                        concurrency::Transaction *txn);

  void CreateOrUpdateZoneMapInCatalog(oid_t database_id, oid_t table_id,
                                      oid_t tile_group_id, oid_t col_itr,
                                      std::string min, std::string max,
                                      std::string type,
                                      concurrency::Transaction *txn);

  std::unique_ptr<ZoneMapManager::ColumnStatistics> GetZoneMapFromCatalog(
      oid_t database_id, oid_t table_id, oid_t tile_group_id, oid_t col_itr);

  bool ComparePredicateAgainstZoneMap(storage::PredicateInfo *parsed_predicates,
                                      int32_t num_predicates,
                                      storage::DataTable *table,
                                      int64_t tile_group_id);

  bool ZoneMapTableExists();

 private:
  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  type::Value GetValueAsOriginal(type::Value val, type::Value actual_type) {
    std::string type_str = (std::string)actual_type.GetData();
    type::TypeId type_id = StringToTypeId(type_str);
    return val.CastAs(type_id);
  }

  std::unique_ptr<ZoneMapManager::ColumnStatistics> GetResultVectorAsZoneMap(
      std::unique_ptr<std::vector<type::Value>> &result_vector);

  bool checkEqual(type::Value predicateVal, ColumnStatistics *stats) {
    return ((stats->min).CompareLessThanEquals(predicateVal)) &&
           ((stats->max).CompareGreaterThanEquals(predicateVal));
  }

  bool checkLessThan(type::Value predicateVal, ColumnStatistics *stats) {
    return predicateVal.CompareGreaterThan(stats->min);
  }

  bool checkLessThanEquals(type::Value predicateVal, ColumnStatistics *stats) {
    return predicateVal.CompareGreaterThanEquals(stats->min);
  }

  bool checkGreaterThan(type::Value predicateVal, ColumnStatistics *stats) {
    return predicateVal.CompareLessThan(stats->max);
  }

  bool checkGreaterThanEquals(type::Value predicateVal,
                              ColumnStatistics *stats) {
    return predicateVal.CompareLessThanEquals(stats->max);
  }

  //===--------------------------------------------------------------------===//
  // Data Members
  //===--------------------------------------------------------------------===//
  std::unique_ptr<type::AbstractPool> pool_;

  bool zone_map_table_exists;
};
}
}
