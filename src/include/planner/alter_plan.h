//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_plan.h
//
// Identification: src/include/parser/alter_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "planner/abstract_plan.h"
#include "parser/alter_statement.h"
namespace peloton {

namespace planner {
/** @brief The plan used for altering
 *  TODO: adding support for add/drop column
 */
class AlterPlan : public AbstractPlan {
 public:
  AlterPlan() = delete;
  explicit AlterPlan(UNUSED_ATTRIBUTE parser::AlterTableStatement *parse_tree) {
    LOG_TRACE("%s", parse_tree->GetInfo().c_str());
  }
  virtual ~AlterPlan() {}

  virtual PlanNodeType GetPlanNodeType() { return PlanNodeType::ALTER; }

  virtual std::unique_ptr<AbstractPlan> Copy() { return nullptr; }
};
}
}
