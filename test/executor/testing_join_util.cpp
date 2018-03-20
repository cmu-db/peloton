//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// join_tests_util.cpp
//
// Identification: test/executor/join_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/testing_join_util.h"

#include "expression/abstract_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "common/internal_types.h"

namespace peloton {
namespace test {

// Create join predicate
expression::AbstractExpression *TestingJoinUtil::CreateJoinPredicate() {
  expression::AbstractExpression *predicate = nullptr;

  // LEFT.1 == RIGHT.1

  expression::TupleValueExpression *left_table_attr_1 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  expression::TupleValueExpression *right_table_attr_1 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 1, 1);

  predicate = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, left_table_attr_1, right_table_attr_1);

  return predicate;
}

std::unique_ptr<const planner::ProjectInfo>
TestingJoinUtil::CreateProjection() {
  // Create the plan node
  TargetList target_list;
  DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 0
  /////////////////////////////////////////////////////////

  // direct map
  DirectMap direct_map1 = std::make_pair(0, std::make_pair(0, 1));
  DirectMap direct_map2 = std::make_pair(1, std::make_pair(1, 1));
  DirectMap direct_map3 = std::make_pair(2, std::make_pair(1, 0));
  DirectMap direct_map4 = std::make_pair(3, std::make_pair(0, 0));

  direct_map_list.push_back(direct_map1);
  direct_map_list.push_back(direct_map2);
  direct_map_list.push_back(direct_map3);
  direct_map_list.push_back(direct_map4);

  return std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list)));
}

// Create complicated join predicate
expression::AbstractExpression *
TestingJoinUtil::CreateComplicatedJoinPredicate() {
  expression::AbstractExpression *predicate = nullptr;

  // LEFT.1 == RIGHT.1

  expression::TupleValueExpression *left_table_attr_1 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  expression::TupleValueExpression *right_table_attr_1 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 1, 1);

  expression::ComparisonExpression *comp_a =
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_EQUAL, left_table_attr_1, right_table_attr_1);

  // LEFT.3 > 50.0

  expression::TupleValueExpression *left_table_attr_3 =
      new expression::TupleValueExpression(type::TypeId::DECIMAL, 0, 1);
  expression::ConstantValueExpression *const_val_1 =
      new expression::ConstantValueExpression(
          type::ValueFactory::GetDecimalValue(50.0));
  expression::ComparisonExpression *comp_b =
      new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN,
                                           left_table_attr_3, const_val_1);

  predicate = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, comp_a, comp_b);

  return predicate;
}

}  // namespace test
}  // namespace peloton
