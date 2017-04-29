//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_workload_q3.cpp
//
// Identification: src/main/tpch/tpch_workload_q3.cpp
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
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"

namespace peloton {
namespace benchmark {
namespace tpch {

static int32_t _1995_03_10 = 794811600;

std::unique_ptr<planner::AbstractPlan> TPCHBenchmark::ConstructQ3Plan() const {
  auto &lineitem = db_.GetTable(TableId::Lineitem);
  auto &customer = db_.GetTable(TableId::Customer);
  auto &orders = db_.GetTable(TableId::Orders);

  //////////////////////////////////////////////////////////////////////////////                                                              [3136/4535]
  /// THE PREDICATE FOR THE SCAN OVER LINEITEM
  //////////////////////////////////////////////////////////////////////////////

  uint32_t machinery = db_.CodeForMktSegment("MACHINERY");

  auto orderdate_pred = std::unique_ptr<expression::AbstractExpression>{
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_LESSTHANOREQUALTO,
          new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 4),
          new expression::ConstantValueExpression(type::ValueFactory::GetDateValue(_1995_03_10)))};

  auto mktsegment_pred = std::unique_ptr<expression::AbstractExpression>{
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_EQUAL,
          new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 6),
          new expression::ConstantValueExpression(type::ValueFactory::GetIntegerValue(machinery)))};

  auto shipdate_pred = std::unique_ptr<expression::AbstractExpression>{
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_GREATERTHAN,
          new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 10),
          new expression::ConstantValueExpression(type::ValueFactory::GetDateValue(_1995_03_10)))};

  //////////////////////////////////////////////////////////////////////////////
  /// THE SCAN PLANS
  //////////////////////////////////////////////////////////////////////////////

  // The table scans
  std::unique_ptr<planner::AbstractPlan> lineitem_scan{
      new planner::SeqScanPlan(&lineitem, shipdate_pred.release(), {0,5,6})};
  std::unique_ptr<planner::AbstractPlan> order_scan{
      new planner::SeqScanPlan(&orders, orderdate_pred.release(), {0,1,4,7})};
  std::unique_ptr<planner::AbstractPlan> customer_scan{
      new planner::SeqScanPlan(&customer, mktsegment_pred.release(), {0})};

  //////////////////////////////////////////////////////////////////////////////
  /// REARRANGE ORDERS COLUMNS FOR JOIN
  //////////////////////////////////////////////////////////////////////////////

  DirectMap dmo1 = std::make_pair(0, std::make_pair(0, 1));
  DirectMap dmo2 = std::make_pair(1, std::make_pair(0, 0));
  DirectMap dmo3 = std::make_pair(2, std::make_pair(0, 2));
  DirectMap dmo4 = std::make_pair(3, std::make_pair(0, 3));
  DirectMapList order_dm = {dmo1, dmo2, dmo3, dmo4};

  std::unique_ptr<const planner::ProjectInfo> order_project_info{
      new planner::ProjectInfo(TargetList{}, std::move(order_dm))};
  auto order_schema = std::shared_ptr<const catalog::Schema>(new catalog::Schema(
      {{type::Type::TypeId::INTEGER, kIntSize, "o_custkey", true},
       {type::Type::TypeId::INTEGER, kIntSize, "o_orderkey", true},
       {type::Type::TypeId::DATE, kDateSize, "o_orderdate", true},
       {type::Type::TypeId::INTEGER, kIntSize, "o_shippriority", true}}));
  std::unique_ptr<planner::AbstractPlan> order_projection{
      new planner::ProjectionPlan(std::move(order_project_info), order_schema)};

  //////////////////////////////////////////////////////////////////////////////
  /// Customer - Order HASH PLAN
  //////////////////////////////////////////////////////////////////////////////

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  hash_keys.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0));

  std::unique_ptr<planner::AbstractPlan> customer_hash_plan{new planner::HashPlan(hash_keys)};

  //////////////////////////////////////////////////////////////////////////////
  /// Customer - Order JOIN
  //////////////////////////////////////////////////////////////////////////////

  DirectMap dm1 = std::make_pair(0, std::make_pair(0, 1));
  DirectMap dm2 = std::make_pair(1, std::make_pair(0, 2));
  DirectMap dm3 = std::make_pair(2, std::make_pair(0, 3));
  DirectMapList direct_map_list = {dm1, dm2, dm3};

  std::unique_ptr<const planner::ProjectInfo> projection{new planner::ProjectInfo(
      TargetList{}, std::move(direct_map_list))};

  auto schema = std::shared_ptr<const catalog::Schema>(new catalog::Schema(
      {{type::Type::TypeId::INTEGER, kIntSize, "o_orderkey", true},
       {type::Type::TypeId::DATE, kDateSize, "o_orderdate", true},
       {type::Type::TypeId::INTEGER, kIntSize, "o_shippriority", true}}));

  std::vector<std::unique_ptr<const expression::AbstractExpression>> left_hash_keys;
  left_hash_keys.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1));

  std::vector<std::unique_ptr<const expression::AbstractExpression>> right_hash_keys;
  right_hash_keys.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 1, 0));

  std::unique_ptr<planner::AbstractPlan> cust_order_hj_plan{new planner::HashJoinPlan(
      JoinType::INNER, nullptr, std::move(projection),
      schema, left_hash_keys, right_hash_keys)};

  //////////////////////////////////////////////////////////////////////////////
  /// ORDER - LINITEM HASH PLAN
  //////////////////////////////////////////////////////////////////////////////

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys2;
  hash_keys2.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0));

  std::unique_ptr<planner::AbstractPlan> cust_order_hash_plan{new planner::HashPlan(hash_keys2)};

  //////////////////////////////////////////////////////////////////////////////
  /// ORDER - LINEITEM JOIN
  //////////////////////////////////////////////////////////////////////////////

  DirectMap dm12 = std::make_pair(0, std::make_pair(0, 0));
  DirectMap dm22 = std::make_pair(1, std::make_pair(0, 1));
  DirectMap dm32 = std::make_pair(2, std::make_pair(0, 2));
  DirectMap dm42 = std::make_pair(3, std::make_pair(1, 0));
  DirectMap dm52 = std::make_pair(4, std::make_pair(1, 1));
  DirectMap dm62 = std::make_pair(5, std::make_pair(1, 2));
  DirectMapList dm_ol = {dm12, dm22, dm32, dm42, dm52, dm62};

  std::unique_ptr<const planner::ProjectInfo> projection2{new planner::ProjectInfo(
      TargetList{}, std::move(dm_ol))};

  auto ol_schema = std::shared_ptr<const catalog::Schema>(new catalog::Schema(
      {{type::Type::TypeId::INTEGER, kIntSize, "l_orderkey", true},
       {type::Type::TypeId::DECIMAL, kDecimalSize, "l_extendedprice", true},
       {type::Type::TypeId::DECIMAL, kDecimalSize, "l_discount", true},
       {type::Type::TypeId::INTEGER, kIntSize, "o_orderkey", true},
       {type::Type::TypeId::DATE, kDateSize, "o_shipddate", true},
       {type::Type::TypeId::INTEGER, kIntSize, "o_shippriority", true}}));

  std::vector<std::unique_ptr<const expression::AbstractExpression>> left_hash_keys2;
  left_hash_keys2.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1));

  std::vector<std::unique_ptr<const expression::AbstractExpression>> right_hash_keys2;
  right_hash_keys2.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 1, 0));

  std::unique_ptr<planner::AbstractPlan> hj_plan2{new planner::HashJoinPlan(
      JoinType::INNER, nullptr, std::move(projection2),
      ol_schema, left_hash_keys2, right_hash_keys2)};

  //////////////////////////////////////////////////////////////////////////////
  /// AGGREGATION
  //////////////////////////////////////////////////////////////////////////////
  planner::AggregatePlan::AggTerm agg1{
      ExpressionType::AGGREGATE_SUM,
      new expression::OperatorExpression{
          ExpressionType::OPERATOR_MULTIPLY, type::Type::TypeId::DECIMAL,
          new expression::TupleValueExpression{type::Type::TypeId::DECIMAL, 0, 1},
          new expression::OperatorExpression{
              ExpressionType::OPERATOR_MINUS, type::Type::TypeId::DECIMAL,
              new expression::ConstantValueExpression{type::ValueFactory::GetDecimalValue(1.0)},
              new expression::TupleValueExpression{type::Type::TypeId::DECIMAL, 0, 2}
          }
      }
  };
  auto agg_out_schema = std::shared_ptr<const catalog::Schema>{
      new catalog::Schema{
          {{type::Type::TypeId::INTEGER, kIntSize, "l_orderkey", true},
           {type::Type::TypeId::DATE, kDateSize, "o_orderdate", true},
           {type::Type::TypeId::INTEGER, kIntSize, "o_shippriority", true},
           {type::Type::TypeId::DECIMAL, kDecimalSize, "revenue", true}}
      }
  };

  DirectMapList dml = {
      {0, {0, 0}}, {1, {0, 4}}, {2, {0, 5}}, {3, {1, 0}}
  };
  std::unique_ptr<const planner::ProjectInfo> agg_project{
      new planner::ProjectInfo(TargetList{}, std::move(dml))};
  std::unique_ptr<planner::AbstractPlan> agg_plan{
      new planner::AggregatePlan(std::move(agg_project), nullptr,
                                 {agg1}, {0, 4, 5}, agg_out_schema,
                                 AggregateType::HASH)};

  //////////////////////////////////////////////////////////////////////////////
  /// SORT
  //////////////////////////////////////////////////////////////////////////////

  std::unique_ptr<planner::AbstractPlan> sort_plan{
      new planner::OrderByPlan{{3,1}, {false, false}, {0,1,2,3}}
  };

  //////////////////////////////////////////////////////////////////////////////
  /// TIE THE SHIT UP
  //////////////////////////////////////////////////////////////////////////////

  // Build hash on customer
  customer_hash_plan->AddChild(std::move(customer_scan));

  // Project order
  order_projection->AddChild(std::move(order_scan));

  // Customer x Orders hash join (from convention, build goes on right)
  cust_order_hj_plan->AddChild(std::move(order_projection));
  cust_order_hj_plan->AddChild(std::move(customer_hash_plan));

  // Build hash on output of cust x order
  cust_order_hash_plan->AddChild(std::move(cust_order_hj_plan));

  // Final join
  hj_plan2->AddChild(std::move(lineitem_scan));
  hj_plan2->AddChild(std::move(cust_order_hash_plan));

  // Aggregation on output of join
  agg_plan->AddChild(std::move(hj_plan2));

  // Sort final results
  sort_plan->AddChild(std::move(agg_plan));

  return sort_plan;

}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton