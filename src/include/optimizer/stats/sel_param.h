//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sel_param.h
//
// Identification: src/include/optimizer/stats/sel_param.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cmath>
#include <vector>
#include <algorithm>

#include "type/value.h"
#include "common/macros.h"
#include "common/logger.h"
#include "storage/data_table.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/stats_util.h"
#include "optimizer/stats/tuple_samples_storage.h"
#include "catalog/column_catalog.h"
#include "catalog/catalog.h"


namespace peloton {
namespace optimizer {

//===----------------------------------------------------------------------===//
// SelParam
//
// Input parameters for computing operator selectivity.
// Required parameters:
//  - database_id
//  - table_id
//  - column_id
//  - value
// Optional parameters:
//  - pattern (string)
//===----------------------------------------------------------------------===//
class SelParam {
public:
  SelParam(oid_t database_id, oid_t table_id, oid_t column_id, const type::Value& value) :
    database_id{database_id},
    table_id{table_id},
    column_id{column_id},
    value{value}
  {}

  oid_t database_id;
  oid_t table_id;
  oid_t column_id;
  const type::Value value;

  // LIKE / NOT LIKE
  std::string pattern = "";
};
} /* namespace optimizer */
} /* namespace peloton */
