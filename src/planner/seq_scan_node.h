/**
 * @brief Header for sequential scan plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <string>

#include "planner/abstract_plan_node.h"
#include "common/types.h"

namespace nstore {
namespace planner {

class SeqScanNode : public AbstractPlanNode {
  SeqScanNode(const SeqScanNode &) = delete;
  SeqScanNode& operator=(const SeqScanNode &) = delete;
  SeqScanNode(SeqScanNode &&) = delete;
  SeqScanNode& operator=(SeqScanNode &&) = delete;

  //TODO Implement constructor.

  inline PlanNodeType GetPlanNodeType() const {
    //TODO Implement.
    return PLAN_NODE_TYPE_INVALID;
  }

  std::string debugInfo(const std::string& spacer) const {
    //TODO Implement.
    (void) spacer;
    return "";
  }
};

} // namespace planner
} // namespace nstore
