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

	void AddChild(AbstractPlanNode *child) {
		children.push_back(child);
	}

	const std::vector<AbstractPlanNode *>& GetChildren() const {
		return children;
	}

  private:

	/// children plan nodes in the plan tree
    std::vector<AbstractPlanNode *> children;
};

} // namespace planner
} // namespace nstore
