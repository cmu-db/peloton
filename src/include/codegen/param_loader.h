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
  static std::vector<type::Value> Load(const planner::AbstractPlan *plan);

 private:

  // ============================
  // Load parameters from a plan.
  // ============================

  static void Load(const planner::AbstractPlan *plan,
                   std::vector<type::Value> *params);

  static void Load(const planner::SeqScanPlan *plan,
                   std::vector<type::Value> *params);

  static void Load(const planner::ProjectionPlan *plan,
                   std::vector<type::Value> *params);

  static void Load(const planner::HashJoinPlan *plan,
                   std::vector<type::Value> *params);

  static void Load(const planner::AggregatePlan *plan,
                   std::vector<type::Value> *params);

  static void Load(const planner::OrderByPlan *plan,
                   std::vector<type::Value> *params);

  static void Load(const planner::DeletePlan *plan,
                   std::vector<type::Value> *params);

  static void Load(const planner::UpdatePlan *plan,
                   std::vector<type::Value> *params);

  static void Load(const planner::InsertPlan *plan,
                   std::vector<type::Value> *params);

  // ===================================
  // Load parameters from an expression.
  // ===================================

  static void Load(const expression::AbstractExpression *expr,
                   std::vector<type::Value> *params);

  static void Load(const expression::ParameterValueExpression *expr,
                   std::vector<type::Value> *params);

  static void Load(const expression::ConstantValueExpression *expr,
                   std::vector<type::Value> *params);

  static void Load(const expression::CaseExpression *expr,
                   std::vector<type::Value> *params);

  // =======================================
  // Load parameters from a projection info.
  // =======================================

  static void Load(const planner::ProjectInfo *projection,
                   std::vector<type::Value> *params);
};

}  // namespace codegen
}  // namespace peloton
