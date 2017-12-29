//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// join_tests_util.h
//
// Identification: test/include/executor/join_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>
#include <memory>

#include "common/internal_types.h"

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

class TestingJoinUtil {
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
