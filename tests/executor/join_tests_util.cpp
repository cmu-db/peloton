//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// join_tests_util.cpp
//
// Identification: tests/executor/join_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/join_tests_util.h"

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/conjunction_expression.h"
#include "backend/planner/project_info.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace test {

// Create join predicate
expression::AbstractExpression *JoinTestsUtil::CreateJoinPredicate() {
  expression::AbstractExpression *predicate = nullptr;

  // LEFT.1 == RIGHT.1

  expression::TupleValueExpression *left_table_attr_1 =
      new expression::TupleValueExpression(0, 1);
  expression::TupleValueExpression *right_table_attr_1 =
      new expression::TupleValueExpression(1, 1);

  predicate = new expression::ComparisonExpression<expression::CmpEq>(
      EXPRESSION_TYPE_COMPARE_EQUAL, left_table_attr_1, right_table_attr_1);

  return predicate;
}

std::unique_ptr<const planner::ProjectInfo> JoinTestsUtil::CreateProjection() {
  // Create the plan node
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 0
  /////////////////////////////////////////////////////////

  // direct map
  planner::ProjectInfo::DirectMap direct_map1 =
      std::make_pair(0, std::make_pair(0, 1));
  planner::ProjectInfo::DirectMap direct_map2 =
      std::make_pair(1, std::make_pair(1, 1));
  planner::ProjectInfo::DirectMap direct_map3 =
      std::make_pair(2, std::make_pair(1, 0));
  planner::ProjectInfo::DirectMap direct_map4 =
      std::make_pair(3, std::make_pair(0, 0));

  direct_map_list.push_back(direct_map1);
  direct_map_list.push_back(direct_map2);
  direct_map_list.push_back(direct_map3);
  direct_map_list.push_back(direct_map4);

  return std::unique_ptr<const planner::ProjectInfo>(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
}

// Create complicated join predicate
expression::AbstractExpression *
JoinTestsUtil::CreateComplicatedJoinPredicate() {
  expression::AbstractExpression *predicate = nullptr;

  // LEFT.1 == RIGHT.1

  expression::TupleValueExpression *left_table_attr_1 =
      new expression::TupleValueExpression(0, 1);
  expression::TupleValueExpression *right_table_attr_1 =
      new expression::TupleValueExpression(1, 1);

  expression::ComparisonExpression<expression::CmpEq> *comp_a =
      new expression::ComparisonExpression<expression::CmpEq>(
          EXPRESSION_TYPE_COMPARE_EQUAL, left_table_attr_1, right_table_attr_1);

  // LEFT.3 > 50.0

  expression::TupleValueExpression *left_table_attr_3 =
      new expression::TupleValueExpression(0, 1);
  expression::ConstantValueExpression *const_val_1 =
      new expression::ConstantValueExpression(
          ValueFactory::GetDoubleValue(50.0));
  expression::ComparisonExpression<expression::CmpGt> *comp_b =
      new expression::ComparisonExpression<expression::CmpGt>(
          EXPRESSION_TYPE_COMPARE_GREATERTHAN, left_table_attr_3, const_val_1);

  predicate = new expression::ConjunctionExpression<expression::ConjunctionAnd>(
      EXPRESSION_TYPE_CONJUNCTION_AND, comp_a, comp_b);

  return predicate;
}

}  // namespace test
}  // namespace peloton
