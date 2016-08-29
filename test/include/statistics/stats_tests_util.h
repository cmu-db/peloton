//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_tests_util.h
//
// Identification: tests/concurrency/transaction_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "harness.h"
#include "backend/catalog/schema.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group_header.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction.h"
#include "backend/common/types.h"
#include "backend/expression/comparison_expression.h"

#pragma once

namespace peloton {
namespace test {

class StatsTestsUtil {
 public:

  static storage::Tuple PopulateTuple(const catalog::Schema *schema,
                                      int first_col_val,
                                      int second_col_val,
                                      int third_col_val,
                                      int fourth_col_val);

};
}
}
