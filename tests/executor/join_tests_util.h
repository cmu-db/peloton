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
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/conjunction_expression.h"
#include "backend/planner/project_info.h"
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

  // Create join predicate
  static expression::AbstractExpression *CreateJoinPredicate();

  // Create projection
  static planner::ProjectInfo *CreateProjection();

};

}  // namespace test
}  // namespace peloton
