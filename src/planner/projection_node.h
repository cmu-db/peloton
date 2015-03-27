#pragma once

#include "planner/abstract_plan_node.h"

namespace nstore {
namespace planner {
    
class ProjectionNode : public AbstractPlanNode {
 public:

  const std::vector<int>& output_column_ids() const {
    return output_column_ids_;
  }

 private:

  // TODO For now we only store column ids. We need to import the whole expression
  // system from voltdb to support parameterized plans and expressions..
  std::vector<int> output_column_ids_;
};

} // namespace planner
} // namespace nstore
