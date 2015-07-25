/**
 * @brief Header for abstract plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <cstdint>
#include <vector>
#include <map>
#include <vector>

#include "backend/common/types.h"

namespace peloton {

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Plan Node
//===--------------------------------------------------------------------===//

class AbstractPlanNode {
 public:
  AbstractPlanNode(const AbstractPlanNode&) = delete;
  AbstractPlanNode& operator=(const AbstractPlanNode&) = delete;
  AbstractPlanNode(AbstractPlanNode&&) = delete;
  AbstractPlanNode& operator=(AbstractPlanNode&&) = delete;

  explicit AbstractPlanNode(oid_t plan_node_id);
  AbstractPlanNode();
  virtual ~AbstractPlanNode();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(AbstractPlanNode* child);

  const std::vector<AbstractPlanNode*>& GetChildren() const;

  AbstractPlanNode* GetParent();

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  oid_t GetPlanNodeId() const;

  void SetPlanNodeId(oid_t plan_node_id);

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual PlanNodeType GetPlanNodeType() const = 0;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Debugging convenience methods
  friend std::ostream& operator<<(std::ostream& os,
                                  const AbstractPlanNode& node);
  std::string GetInfo(std::string spacer) const;

  // Override in derived plan nodes
  virtual std::string GetInfo() const;

 private:
  // Every plan node will have a unique id assigned to it at compile time
  oid_t plan_node_id_;

  // A node can have multiple children and parents
  std::vector<AbstractPlanNode*> children_;

  AbstractPlanNode* parent_ = nullptr;
};

}  // namespace planner
}  // namespace peloton
