//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_plan_node.h
//
// Identification: src/backend/planner/abstract_plan_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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

class AbstractPlan {
 public:
  AbstractPlan(const AbstractPlan &) = delete;
  AbstractPlan &operator=(const AbstractPlan &) = delete;
  AbstractPlan(AbstractPlan &&) = delete;
  AbstractPlan &operator=(AbstractPlan &&) = delete;

  explicit AbstractPlan(oid_t plan_node_id);
  AbstractPlan();
  virtual ~AbstractPlan();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(AbstractPlan *child);

  const std::vector<AbstractPlan *> &GetChildren() const;

  AbstractPlan *GetParent();

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
  friend std::ostream &operator<<(std::ostream &os,
                                  const AbstractPlan &node);
  std::string GetInfo(std::string spacer) const;

  // Override in derived plan nodes
  virtual std::string GetInfo() const;

 private:
  // Every plan node will have a unique id assigned to it at compile time
  oid_t plan_node_id_;

  // A node can have multiple children and parents
  std::vector<AbstractPlan *> children_;

  AbstractPlan *parent_ = nullptr;
};

}  // namespace planner
}  // namespace peloton
