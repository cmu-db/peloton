//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_tests_util.h
//
// Identification: tests/include/statistics/stats_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "catalog/schema.h"
#include "storage/tile_group_factory.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "storage/tile_group_header.h"
#include "index/index.h"
#include "index/index_factory.h"
#include "concurrency/transaction_manager_factory.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/transaction.h"
#include "common/types.h"
#include "expression/comparison_expression.h"

#pragma once

namespace peloton {
namespace test {

class StatsTestsUtil {
 public:
  static storage::Tuple PopulateTuple(const catalog::Schema *schema,
                                      int first_col_val, int second_col_val,
                                      int third_col_val, int fourth_col_val);
};

}  // namespace test
}  // namespace peloton
