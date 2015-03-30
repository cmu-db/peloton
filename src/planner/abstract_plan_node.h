#pragma once

#include <cstdint>
#include <vector>

namespace nstore {
namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Plan Node
//===--------------------------------------------------------------------===//

class AbstractPlanNode {
 public:
  virtual ~AbstractPlanNode() {}

  void AddChild(AbstractPlanNode *child) {
    children_.push_back(child);
  }

  const std::vector<AbstractPlanNode *>& children() const {
    return children_;
  }

 private:
  // children plan nodes in the plan tree
  std::vector<AbstractPlanNode *> children_;
};

} // namespace planner
} // namespace nstore
