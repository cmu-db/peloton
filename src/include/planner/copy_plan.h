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

  void SetParameterValues(UNUSED_ATTRIBUTE std::vector<common::Value> *values) {
    // XXX Ignore prepared statement for COPY for now
  }

  const std::string GetInfo() const { return "CopyPlan"; }

  // TODO: Implement copy mechanism
  std::unique_ptr<AbstractPlan> Copy() const { return nullptr; }

  // The path of the target file
  std::string file_path;

  // Whether we're copying the parameters which require deserialization
  bool deserialize_parameters = false;
};

}  // namespace planner
}  // namespace peloton
