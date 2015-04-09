/**
 * @brief Header for abstract plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <cstdint>
#include <vector>

namespace nstore {
namespace planner {

class AbstractPlanNode {
 public:
  virtual ~AbstractPlanNode();

  const std::vector<AbstractPlanNode *>& children() const;

 protected:
  explicit AbstractPlanNode(std::vector<AbstractPlanNode *> &&children);

 private:
  /** @brief Children plan nodes in the plan tree. */
  std::vector<AbstractPlanNode *> children_;
};

} // namespace planner
} // namespace nstore
