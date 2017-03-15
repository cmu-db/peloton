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
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/aggregate_plan.h"
#include "planner/order_by_plan.h"
#include "planner/seq_scan_plan.h"


namespace peloton {
namespace benchmark {
namespace tpch {

static constexpr int32_t _1997_01_01 = 852094800;
static constexpr int32_t _1998_01_01 = 883630800;

std::unique_ptr<planner::AbstractPlan> TPCHBenchmark::ConstructQ6Plan() const {
  auto &lineitem = db_.GetTable(TableId::Lineitem);

  //////////////////////////////////////////////////////////////////////////////
  /// THE PREDICATE FOR THE SCAN OVER LINEITEM
  //////////////////////////////////////////////////////////////////////////////

  auto shipdate_gte = std::unique_ptr<expression::AbstractExpression>{
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_GREATERTHANOREQUALTO,
          new expression::TupleValueExpression(type::Type::TypeId::DATE, 0, 10),
          new expression::ConstantValueExpression(
              type::ValueFactory::GetDateValue(_1997_01_01)))};

  auto shipdate_lt = std::unique_ptr<expression::AbstractExpression>{
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_LESSTHAN,
          new expression::TupleValueExpression(type::Type::TypeId::DATE, 0, 10),
          new expression::ConstantValueExpression(
              type::ValueFactory::GetDateValue(_1998_01_01)))};

  auto discount_gt = std::unique_ptr<expression::AbstractExpression>{
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_GREATERTHAN,
          new expression::TupleValueExpression(type::Type::TypeId::DECIMAL, 0, 6),
          new expression::OperatorExpression(
              ExpressionType::OPERATOR_MINUS, type::Type::TypeId::DECIMAL,
              new expression::ConstantValueExpression(type::ValueFactory::GetDecimalValue(0.07)),
              new expression::ConstantValueExpression(type::ValueFactory::GetDecimalValue(0.01))
          )
      )
  };

  auto discount_lt = std::unique_ptr<expression::AbstractExpression>{
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_LESSTHAN,
          new expression::TupleValueExpression(type::Type::TypeId::DECIMAL, 0, 6),
          new expression::OperatorExpression(
              ExpressionType::OPERATOR_PLUS, type::Type::TypeId::DECIMAL,
              new expression::ConstantValueExpression(type::ValueFactory::GetDecimalValue(0.07)),
              new expression::ConstantValueExpression(type::ValueFactory::GetDecimalValue(0.01))
          )
      )
  };

  auto quantity_lt = std::unique_ptr<expression::AbstractExpression>{
      new expression::ComparisonExpression(
          ExpressionType::COMPARE_LESSTHAN,
          new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 4),
          new expression::ConstantValueExpression(type::ValueFactory::GetIntegerValue(24))
      )
  };

  auto lineitem_pred = std::unique_ptr<expression::AbstractExpression>{
      new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND,
          quantity_lt.release(),
          new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND,
              new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, shipdate_gte.release(), shipdate_lt.release()),
              new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, discount_gt.release(), discount_lt.release())
          )
      )
  };

  //////////////////////////////////////////////////////////////////////////////
  /// THE SCAN PLAN
  //////////////////////////////////////////////////////////////////////////////

  // Lineitem scan
  auto lineitem_scan = std::unique_ptr<planner::AbstractPlan>{
      new planner::SeqScanPlan(&lineitem, lineitem_pred.release(), {4,6,10})};

  return lineitem_scan;
}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton