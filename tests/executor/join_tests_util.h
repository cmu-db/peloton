//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// join_tests_util.h
//
// Identification: tests/executor/join_tests_util.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <memory>

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/storage/data_table.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}

namespace storage {
class DataTable;
class Tuple;
}

namespace test {

class JoinTestsUtil {
 public:
  /** @brief Helper method for performing join tests */
  static bool ExecuteJoinTest(
      peloton::storage::DataTable *leftTable,
      peloton::storage::DataTable *rightTable,
      peloton::storage::DataTable *expected, peloton::PelotonJoinType joinType,
      peloton::expression::AbstractExpression *predicate);
};

}  // namespace test
}  // namespace peloton
