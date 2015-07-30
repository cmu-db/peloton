/**
 * @brief Header for limit plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/planner/abstract_plan_node.h"

namespace peloton {
namespace planner {

/**
 * @brief	Limit (with Offset) plan node.
 * IMPORTANT: now only works on logical_tile and returns with the same schema as
 * the input.
 */
class LimitNode : public AbstractPlanNode {
 public:
  LimitNode(const LimitNode &) = delete;
  LimitNode &operator=(const LimitNode &) = delete;
  LimitNode(LimitNode &&) = delete;
  LimitNode &operator=(LimitNode &&) = delete;

  LimitNode(size_t limit, size_t offset) : limit_(limit), offset_(offset) {}

  // Accessors
  size_t GetLimit() const { return limit_; }

  size_t GetOffset() const { return offset_; }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_LIMIT; }

  inline std::string GetInfo() const { return "Limit"; }

 private:
  const size_t limit_;   // as LIMIT in SQL standard
  const size_t offset_;  // as OFFSET in SQL standard
};

} /* namespace planner */
} /* namespace peloton */
