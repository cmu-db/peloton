//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_workload_q1.cpp
//
// Identification: src/main/tpch/tpch_workload_q1.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/tpch/tpch_workload.h"

#include "concurrency/transaction_manager_factory.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/aggregate_plan.h"
#include "planner/order_by_plan.h"
#include "planner/seq_scan_plan.h"

namespace peloton {
namespace benchmark {
namespace tpch {

static constexpr int32_t _1998_08_28 = 904276800;

std::unique_ptr<planner::AbstractPlan> TPCHBenchmark::ConstructQ1Plan() const {
  auto &lineitem = db_.GetTable(TableId::Lineitem);

  //////////////////////////////////////////////////////////////////////////////
  /// THE PREDICATE FOR THE SCAN OVER LINEITEM
  //////////////////////////////////////////////////////////////////////////////

  auto shipdate_predicate = std::unique_ptr<expression::AbstractExpression>{
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_LESSTHANOREQUALTO,
          new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0,
                                               10),
          new expression::ConstantValueExpression(
              type::ValueFactory::GetDateValue(_1998_08_28)))};

  //////////////////////////////////////////////////////////////////////////////
  /// THE SCAN PLAN
  //////////////////////////////////////////////////////////////////////////////

  // Lineitem scan
  std::unique_ptr<planner::AbstractPlan> lineitem_scan{new planner::SeqScanPlan(
      &lineitem, shipdate_predicate.release(), {8, 9, 4, 5, 6, 7})};

  //////////////////////////////////////////////////////////////////////////////
  /// THE AGGREGATION PLAN
  /////////////////////////////////////////////////////////////////////////////

  // sum(l_quantity) as sum_qty
  planner::AggregatePlan::AggTerm agg1{
      ExpressionType::AGGREGATE_SUM,
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 2)};
  agg1.agg_ai.type = type::Type::TypeId::INTEGER;

  // sum(l_extendedprice) as sum_base_price
  planner::AggregatePlan::AggTerm agg2{
      ExpressionType::AGGREGATE_SUM,
      new expression::TupleValueExpression(type::Type::TypeId::DECIMAL, 0, 3)};
  agg2.agg_ai.type = type::Type::TypeId::DECIMAL;

  // sum(l_extendedprice * (1 - l_discount)) as sum_disc_price
  planner::AggregatePlan::AggTerm agg3{
      ExpressionType::AGGREGATE_SUM,
      new expression::OperatorExpression(
          ExpressionType::OPERATOR_MULTIPLY, type::Type::TypeId::DECIMAL,
          new expression::TupleValueExpression(type::Type::TypeId::DECIMAL, 0,
                                               3),
          new expression::OperatorExpression(
              ExpressionType::OPERATOR_MINUS, type::Type::TypeId::DECIMAL,
              new expression::ConstantValueExpression(
                  type::ValueFactory::GetDecimalValue(1.0)),
              new expression::TupleValueExpression(type::Type::TypeId::DECIMAL,
                                                   0, 4)))};
  agg3.agg_ai.type = type::Type::TypeId::DECIMAL;

  // sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))
  planner::AggregatePlan::AggTerm agg4{
      ExpressionType::AGGREGATE_SUM,
      new expression::OperatorExpression(
          ExpressionType::OPERATOR_MULTIPLY, type::Type::TypeId::DECIMAL,
          // l_extendedprice
          new expression::TupleValueExpression(type::Type::TypeId::DECIMAL, 0,
                                               3),
          // (1 - l_discount) * (1 + l_tax)
          new expression::OperatorExpression(
              ExpressionType::OPERATOR_MULTIPLY, type::Type::TypeId::DECIMAL,
              // 1 - l_discount
              new expression::OperatorExpression(
                  ExpressionType::OPERATOR_MINUS, type::Type::TypeId::DECIMAL,
                  new expression::ConstantValueExpression(
                      type::ValueFactory::GetDecimalValue(1.0)),
                  new expression::TupleValueExpression(
                      type::Type::TypeId::DECIMAL, 0, 4)),
              // 1 + l_tax
              new expression::OperatorExpression(
                  ExpressionType::OPERATOR_PLUS, type::Type::TypeId::DECIMAL,
                  new expression::ConstantValueExpression(
                      type::ValueFactory::GetDecimalValue(1.0)),
                  new expression::TupleValueExpression(
                      type::Type::TypeId::DECIMAL, 0, 5))))};
  agg4.agg_ai.type = type::Type::TypeId::DECIMAL;

  // avg(l_quantity)
  planner::AggregatePlan::AggTerm agg5{
      ExpressionType::AGGREGATE_AVG,
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 2)};
  agg5.agg_ai.type = type::Type::TypeId::BIGINT;

  // avg(l_extendedprice)
  planner::AggregatePlan::AggTerm agg6{
      ExpressionType::AGGREGATE_AVG,
      new expression::TupleValueExpression(type::Type::TypeId::DECIMAL, 0, 3)};
  agg6.agg_ai.type = type::Type::TypeId::DECIMAL;

  // avg(l_discount)
  planner::AggregatePlan::AggTerm agg7{
      ExpressionType::AGGREGATE_AVG,
      new expression::TupleValueExpression(type::Type::TypeId::DECIMAL, 0, 4)};
  agg7.agg_ai.type = type::Type::TypeId::DECIMAL;

  // count(*)
  planner::AggregatePlan::AggTerm agg8{ExpressionType::AGGREGATE_COUNT_STAR,
                                       nullptr};
  agg8.agg_ai.type = type::Type::TypeId::BIGINT;

  auto output_schema =
      std::shared_ptr<const catalog::Schema>{new catalog::Schema(
          {{type::Type::TypeId::INTEGER, kIntSize, "l_returnflag"},
           {type::Type::TypeId::INTEGER, kIntSize, "l_linestatus"},
           {type::Type::TypeId::INTEGER, kIntSize, "sum_qty"},
           {type::Type::TypeId::DECIMAL, kDecimalSize, "sum_base_price"},
           {type::Type::TypeId::DECIMAL, kDecimalSize, "sum_disc_price"},
           {type::Type::TypeId::DECIMAL, kDecimalSize, "sum_charge"},
           {type::Type::TypeId::BIGINT, kBigIntSize, "avg_qty"},
           {type::Type::TypeId::DECIMAL, kDecimalSize, "avg_price"},
           {type::Type::TypeId::DECIMAL, kDecimalSize, "avg_disc"},
           {type::Type::TypeId::BIGINT, kBigIntSize, "count_order"}})};

  DirectMapList dml = {{0, {0, 0}},
                       {1, {0, 1}},
                       {2, {1, 0}},
                       {3, {1, 1}},
                       {4, {1, 2}},
                       {5, {1, 3}},
                       {6, {1, 4}},
                       {7, {1, 5}},
                       {8, {1, 6}},
                       {9, {1, 7}}};

  std::unique_ptr<const planner::ProjectInfo> agg_project{
      new planner::ProjectInfo(TargetList{}, std::move(dml))};
  auto agg_terms = {agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8};
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(agg_project), nullptr, std::move(agg_terms), {0, 1},
      output_schema, AggregateType::HASH)};

  std::unique_ptr<planner::AbstractPlan> sort_plan{new planner::OrderByPlan{
      {0, 1}, {false, false}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}};

  agg_plan->AddChild(std::move(lineitem_scan));
  sort_plan->AddChild(std::move(agg_plan));

  return sort_plan;
}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton