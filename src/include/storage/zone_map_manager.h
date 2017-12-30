//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_manager.h
//
// Identification: src/include/storage/zone_map_manager.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <sstream>

#include "common/macros.h"
#include "common/internal_types.h"
#include "type/value_factory.h"
#include "concurrency/transaction_context.h"
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
                              concurrency::TransactionContext *txn);

  void CreateOrUpdateZoneMapForTileGroup(storage::DataTable *table, 
                                         oid_t tile_group_idx,
                                        concurrency::TransactionContext *txn);

  void CreateOrUpdateZoneMapInCatalog(oid_t database_id, oid_t table_id,
                                      oid_t tile_group_id, oid_t col_itr,
                                      std::string min, std::string max,
                                      std::string type,
                                      concurrency::TransactionContext *txn);

  std::unique_ptr<ZoneMapManager::ColumnStatistics> GetZoneMapFromCatalog(
      oid_t database_id, oid_t table_id, oid_t tile_group_id, oid_t col_itr);

  bool ShouldScanTileGroup(storage::PredicateInfo *parsed_predicates,
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
    return ((stats->min).CompareLessThanEquals(predicateVal)) == CmpBool::TRUE &&
           ((stats->max).CompareGreaterThanEquals(predicateVal) == CmpBool::TRUE);
  }

  bool checkLessThan(type::Value predicateVal, ColumnStatistics *stats) {
    return (predicateVal.CompareGreaterThan(stats->min) == CmpBool::TRUE);
  }

  bool checkLessThanEquals(type::Value predicateVal, ColumnStatistics *stats) {
    return (predicateVal.CompareGreaterThanEquals(stats->min) == CmpBool::TRUE);
  }

  bool checkGreaterThan(type::Value predicateVal, ColumnStatistics *stats) {
    return (predicateVal.CompareLessThan(stats->max) == CmpBool::TRUE);
  }

  bool checkGreaterThanEquals(type::Value predicateVal,
                              ColumnStatistics *stats) {
    return (predicateVal.CompareLessThanEquals(stats->max) == CmpBool::TRUE);
  }

  //===--------------------------------------------------------------------===//
  // Data Members
  //===--------------------------------------------------------------------===//
  std::unique_ptr<type::AbstractPool> pool_;

  bool zone_map_table_exists;
};
}
}
