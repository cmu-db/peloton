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

#include "nodes/nodes.h"

namespace peloton {

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Plan
//===--------------------------------------------------------------------===//

class AbstractPlan {
 public:
  AbstractPlan(const AbstractPlan &) = delete;
  AbstractPlan &operator=(const AbstractPlan &) = delete;
  AbstractPlan(AbstractPlan &&) = delete;
  AbstractPlan &operator=(AbstractPlan &&) = delete;

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

  // A plan node can have multiple children
  std::vector<AbstractPlan *> children_;

  AbstractPlan *parent_ = nullptr;
};

//===--------------------------------------------------------------------===//
// Abstract Plan State
//===--------------------------------------------------------------------===//

class AbstractPlanState {
 public:
  AbstractPlanState(const AbstractPlanState &) = delete;
  AbstractPlanState &operator=(const AbstractPlanState &) = delete;
  AbstractPlanState(AbstractPlan &&) = delete;
  AbstractPlanState &operator=(AbstractPlanState &&) = delete;

  explicit AbstractPlanState(NodeTag plan_state_id);
  virtual ~AbstractPlanState();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(AbstractPlanState *child);

  const std::vector<AbstractPlanState *> &GetChildren() const;

  AbstractPlanState *GetParent();

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  virtual NodeTag GetNodeTag() const = 0;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Debugging convenience methods
  friend std::ostream &operator<<(std::ostream &os,
                                  const AbstractPlanState &node);

 private:

  // type of the plan state
  NodeTag type_;

  // A plan state can have multiple children
  std::vector<AbstractPlanState *> children_;

  AbstractPlanState *parent_ = nullptr;
};

}  // namespace planner
}  // namespace peloton
