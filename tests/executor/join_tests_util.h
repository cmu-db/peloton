//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// join_tests_util.h
//
// Identification: tests/executor/join_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <memory>

#include "backend/common/types.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}

namespace planner {
class ProjectInfo;
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
  static std::unique_ptr<const planner::ProjectInfo> CreateProjection();

  // Create complicated join predicate
  static expression::AbstractExpression *CreateComplicatedJoinPredicate();
};

}  // namespace test
}  // namespace peloton
