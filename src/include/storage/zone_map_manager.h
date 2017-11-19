//===----------------------------------------------------------------------===//
//
//                         Peloton
//
//
//
// Identification : 
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sstream>

#include "common/macros.h"
#include "type/types.h"
#include "type/value_factory.h"

namespace peloton {
namespace storage {

class ZoneMapManager {

  public:
    typedef struct ColumnStatistics{
      type::Value min;
      type::Value max;
    } ColumnStatistics;

  // Global Singleton
  static ZoneMapManager *GetInstance();

  ZoneMapManager();
  
  void CreateZoneMapTableInCatalog();
  
  void CreateZoneMapsForTable( storage::DataTable *table, concurrency::Transaction *txn);
  
  void CreateOrUpdateZoneMapForTileGroup(oid_t database_id, oid_t table_id, 
  oid_t tile_group_id, storage::TileGroup *tile_group, size_t num_columns, 
  concurrency::Transaction *txn);
  
  void CreateOrUpdateZoneMapinCatalog(oid_t database_id, oid_t table_id, 
  oid_t tile_group_id, oid_t col_itr, std::string min, std::string max, std::string type, 
  concurrency::Transaction *txn);

  std::shared_ptr<ZoneMapManager::ColumnStatistics> GetZoneMapFromCatalog(oid_t database_id, oid_t table_id, 
    oid_t tile_group_id, oid_t col_itr);
  
  bool ComparePredicate(storage::PredicateInfo *parsed_predicates , int32_t num_predicates, 
    storage::DataTable *table, int64_t tile_group_id);

  private:

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  type::Value GetValueAsOriginal(type::Value val, type::Value actual_type) {
    std::string type_str = (std::string)actual_type.GetData();
    type::TypeId type_id = StringToTypeId(type_str);
    return val.CastAs(type_id);
  }

  std::shared_ptr<ZoneMapManager::ColumnStatistics> GetResultVectorAsZoneMap(std::unique_ptr<std::vector<type::Value>> &result_vector);

  bool checkEqual(type::Value predicateVal, ColumnStatistics *stats) {

    return ((stats->min).CompareLessThanEquals(predicateVal)) && ((stats->max).CompareGreaterThanEquals(predicateVal));
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

  bool checkGreaterThanEquals(type::Value predicateVal, ColumnStatistics *stats) {
    return predicateVal.CompareLessThanEquals(stats->max);
  }

  //===--------------------------------------------------------------------===//
  // Data Members
  //===--------------------------------------------------------------------===//
   std::unique_ptr<type::AbstractPool> pool_;

};

}
}
