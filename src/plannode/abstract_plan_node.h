#pragma once

namespace nstore {
namespace plannode {

class AbstractPlanNode {
  public:
    void addChild(AbstractPlanNode *child) { children_.push_back(child); }
    const std::vector<int32_t>& getChildren() const { return children_; } 

  private:
    std::vector<AbstractPlanNode *> children_;
};

} // namespace plannode
} // namespace nstore
