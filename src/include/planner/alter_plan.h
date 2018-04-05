//
// Created by Nevermore on 03/04/2018.
//

#pragma once
#include "planner/abstract_plan.h"
#include "parser/alter_statement.h"
namespace peloton {

namespace planner {
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
