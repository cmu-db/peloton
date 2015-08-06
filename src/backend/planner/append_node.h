//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// append_node.h
//
// Identification: src/backend/planner/append_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"
#include "backend/planner/abstract_plan_node.h"

namespace peloton {
namespace planner {

/**
 * @brief Plan node for append.
 */
class AppendNode : public AbstractPlanNode {
 public:
  AppendNode(const AppendNode &) = delete;
  AppendNode &operator=(const AppendNode &) = delete;
  AppendNode(const AppendNode &&) = delete;
  AppendNode &operator=(const AppendNode &&) = delete;

  AppendNode() {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_APPEND; }

  inline std::string GetInfo() const { return "Append"; }

 private:
  // nothing
};
}
}
