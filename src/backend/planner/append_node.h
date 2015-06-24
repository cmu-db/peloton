/**
 * @brief Header for append node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/common/types.h"
#include "backend/planner/abstract_plan_node.h"

namespace nstore {
namespace planner {

/**
 * @brief Plan node for append.
 */
class AppendNode : public AbstractPlanNode {
 public:
  AppendNode(const AppendNode &) = delete;
  AppendNode& operator=(const AppendNode &) = delete;
  AppendNode(const AppendNode &&) = delete;
  AppendNode& operator=(const AppendNode &&) = delete;

  AppendNode() {

  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_APPEND;
  }

  inline std::string GetInfo() const {
    return "Append";
  }

 private:
  // nothing

};

}
}
