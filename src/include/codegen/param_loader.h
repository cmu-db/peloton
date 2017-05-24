//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// param_loader.h
//
// Identification: src/include/codegen/param_loader.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <unordered_map>
#include "codegen/parameter.h"
#include "expression/abstract_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/case_expression.h"
#include "type/value.h"
#include "planner/abstract_plan.h"
#include "planner/aggregate_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/projection_plan.h"
#include "planner/order_by_plan.h"
#include "planner/update_plan.h"

namespace peloton {
namespace codegen {

class ParamLoader {
 public:
  static std::tuple<
      std::unordered_map<const expression::AbstractExpression *, size_t>,
      std::unordered_map<int, size_t>,
      std::vector<Parameter>>
  LoadParams(const planner::AbstractPlan *plan);

 private:
  ParamLoader() : num_params_(0) {}

  // ============================
  // Load parameters from a plan.
  // ============================

  void Load(const planner::AbstractPlan *plan);

  void Load(const planner::SeqScanPlan *plan);

  void Load(const planner::ProjectionPlan *plan);

  void Load(const planner::HashJoinPlan *plan);

  void Load(const planner::AggregatePlan *plan);

  void Load(const planner::OrderByPlan *plan);

  void Load(const planner::DeletePlan *plan);

  void Load(const planner::UpdatePlan *plan);

  void Load(const planner::InsertPlan *plan);

  // ===================================
  // Load parameters from an expression.
  // ===================================

  void Load(const expression::AbstractExpression *expr);

  void Load(const expression::ParameterValueExpression *expr);

  void Load(const expression::ConstantValueExpression *expr);

  void Load(const expression::CaseExpression *expr);

  // =======================================
  // Load parameters from a projection info.
  // =======================================

  void Load(const planner::ProjectInfo *projection);

  size_t num_params_;

  std::unordered_map<const expression::AbstractExpression *, size_t> const_ids_;

  std::unordered_map<int, size_t> value_ids_;

  std::vector<Parameter> params_;
};

}  // namespace codegen
}  // namespace peloton
