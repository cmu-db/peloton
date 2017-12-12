//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_to_plan_transformer.cpp
//
// Identification: src/optimizer/operator_to_plan_transformer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/plan_generator.h"

#include "optimizer/operator_expression.h"
#include "planner/aggregate_plan.h"
#include "planner/delete_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "planner/limit_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/update_plan.h"
#include "settings/settings_manager.h"
#include "storage/data_table.h"

using std::vector;
using std::make_pair;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::shared_ptr;
using std::move;
using std::make_tuple;
using std::make_pair;
using std::pair;

namespace peloton {
namespace optimizer {

PlanGenerator::PlanGenerator() {}

unique_ptr<planner::AbstractPlan> PlanGenerator::ConvertOpExpression(
    shared_ptr<OperatorExpression> op, shared_ptr<PropertySet> required_props,
    vector<expression::AbstractExpression *> required_cols,
    vector<expression::AbstractExpression *> output_cols,
    vector<unique_ptr<planner::AbstractPlan>> &children_plans,
    vector<ExprMap> children_expr_map) {
  required_props_ = required_props;
  required_cols_ = required_cols;
  output_cols_ = output_cols;
  children_plans_ = move(children_plans);
  children_expr_map_ = move(children_expr_map);
  op->Op().Accept(this);
  return move(output_plan_);
}

void PlanGenerator::Visit(const DummyScan *) {
  // DummyScan is used in case of SELECT without FROM so that enforcer
  // can enforce a PhysicalProjection on top of DummyScan to generate correct
  // result. But here, no need to translate DummyScan to any physical plan.
  output_plan_ = nullptr;
}

void PlanGenerator::Visit(const PhysicalSeqScan *) {}

void PlanGenerator::Visit(const PhysicalIndexScan *) {}

void PlanGenerator::Visit(const QueryDerivedScan *) {}

void PlanGenerator::Visit(const PhysicalProject *) {}

void PlanGenerator::Visit(const PhysicalLimit *) {}

void PlanGenerator::Visit(const PhysicalOrderBy *) {}

void PlanGenerator::Visit(const PhysicalHashGroupBy *) {}

void PlanGenerator::Visit(const PhysicalSortGroupBy *) {}

void PlanGenerator::Visit(const PhysicalAggregate *) {}

void PlanGenerator::Visit(const PhysicalDistinct *) {}

void PlanGenerator::Visit(const PhysicalFilter *) {}

void PlanGenerator::Visit(const PhysicalInnerNLJoin *) {}

void PlanGenerator::Visit(const PhysicalLeftNLJoin *) {}

void PlanGenerator::Visit(const PhysicalRightNLJoin *) {}

void PlanGenerator::Visit(const PhysicalOuterNLJoin *) {}

void PlanGenerator::Visit(const PhysicalInnerHashJoin *) {}

void PlanGenerator::Visit(const PhysicalLeftHashJoin *) {}

void PlanGenerator::Visit(const PhysicalRightHashJoin *) {}

void PlanGenerator::Visit(const PhysicalOuterHashJoin *) {}

void PlanGenerator::Visit(const PhysicalInsert *) {}

void PlanGenerator::Visit(const PhysicalInsertSelect *) {}

void PlanGenerator::Visit(const PhysicalDelete *) {}

void PlanGenerator::Visit(const PhysicalUpdate *) {}

}  // namespace optimizer
}  // namespace peloton
