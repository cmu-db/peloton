//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_plan.h
//
// Identification: src/include/planner/copy_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "parser/statement_copy.h"
#include "parser/statement_select.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace parser {
class CopyStatement;
}

namespace planner {
class CopyPlan : public AbstractPlan {
 public:
  CopyPlan() = delete;
  CopyPlan(const CopyPlan &) = delete;
  CopyPlan &operator=(const CopyPlan &) = delete;
  CopyPlan(CopyPlan &&) = delete;
  CopyPlan &operator=(CopyPlan &&) = delete;

  explicit CopyPlan(parser::CopyStatement *parse_tree);

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_COPY; }

  void SetParameterValues(std::vector<common::Value> *values) {
    // Ignore prepared statement for COPY for now
    (void)values;
  }

  const std::string GetInfo() const { return "CopyPlan"; }

  // TODO: Add copying mechanism
  std::unique_ptr<AbstractPlan> Copy() const { return nullptr; }

 private:
  std::string file_path_;
};

}  // namespace planner
}  // namespace peloton
