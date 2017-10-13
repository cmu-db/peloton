//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map.h
//
// Identification: src/include/storage/zone_map.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <cstring>

#include "common/item_pointer.h"
#include "common/macros.h"
#include "common/platform.h"
#include "common/printable.h"
#include "storage/tuple.h"
#include "type/types.h"
#include "type/value.h"
#include "codegen/zone_map.h"

namespace peloton {
namespace storage {

class ZoneMap;

class ZoneMap : public Printable {

 public:
  ZoneMap() {}
  ~ZoneMap();

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//
  bool ComparePredicate(storage::PredicateInfo *parsed_predicates , int32_t num_predicates);

  void UpdateZoneMap(oid_t tile_column_itr, type::Value val) ;

  inline int32_t GetMinValue(oid_t tile_column_itr) {
    return stats_map[tile_column_itr].min.GetAs<int32_t>();
  }

  inline int32_t GetMaxValue(oid_t tile_column_itr) {
    return stats_map[tile_column_itr].max.GetAs<int32_t>();
  }

  inline bool IsCreated() {
    return zone_map_created;
  }

  inline int GetFakeMin() {
    return min;
  }

  inline int GetFakeMax() {
    return max;
  }

  type::Value GetMinValue_(int colId) {
    return stats_map[colId].min;
  }

  type::Value GetMaxValue_(int colId) {
    return stats_map[colId].max;
  }

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//
  const std::string GetInfo() const;

  void TestCall();

  bool checkEqual(int colId, type::Value predicateVal) {
    return (stats_map[colId].min.CompareLessThanEquals(predicateVal)) && (stats_map[colId].max.CompareGreaterThanEquals(predicateVal));
  }

  bool checkLessThan(int colId, type::Value predicateVal) {
    return predicateVal.CompareGreaterThan(stats_map[colId].min);
  }

  bool checkLessThanEquals(int colId, type::Value predicateVal) {
    return predicateVal.CompareGreaterThanEquals(stats_map[colId].min);
  }

  bool checkGreaterThan(int colId, type::Value predicateVal) {
    return predicateVal.CompareLessThan(stats_map[colId].max);
  }

  bool checkGreaterThanEquals(int colId, type::Value predicateVal) {
    return predicateVal.CompareLessThanEquals(stats_map[colId].max);
  }

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
 	struct statistics{
 		type::Value min;
 		type::Value max;
 	};

 	std::map<oid_t, statistics> stats_map;
 	oid_t num_columns;
  bool zone_map_created = false;
  int min, max;
};


  struct PredicateInfo {
    int col_id;
    int comparison_operator;
    type::Value predicate_value;
  };

}  // namespace storage
}  // namespace peloton