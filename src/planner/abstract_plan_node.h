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

#include "common/types.h"

namespace nstore {

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
  AbstractPlanNode(const AbstractPlanNode &) = delete;
  AbstractPlanNode& operator=(const AbstractPlanNode &) = delete;
  AbstractPlanNode(AbstractPlanNode &&) = delete;
  AbstractPlanNode& operator=(AbstractPlanNode &&) = delete;

  explicit AbstractPlanNode(oid_t plan_node_id);
  AbstractPlanNode();
  virtual ~AbstractPlanNode();


  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(AbstractPlanNode* child);

  std::vector<AbstractPlanNode*>& GetChildren();

  std::vector<oid_t>& GetChildrenIds();

  const std::vector<AbstractPlanNode*>& GetChildren() const;

  void AddParent(AbstractPlanNode* parent);

  std::vector<AbstractPlanNode*>& GetParents();

  std::vector<oid_t>& GetParentIds();

  const std::vector<AbstractPlanNode*>& GetParents() const;

  //===--------------------------------------------------------------------===//
  // Inlined plannodes
  //===--------------------------------------------------------------------===//

  void AddInlinePlanNode(AbstractPlanNode* inline_node);

  AbstractPlanNode* GetInlinePlanNodes(PlanNodeType type) const;

  std::map<PlanNodeType, AbstractPlanNode*>& GetInlinePlanNodes();

  const std::map<PlanNodeType, AbstractPlanNode*>& GetInlinePlanNodes() const;

  bool IsInlined() const;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  oid_t GetPlanNodeId() const;

  void SetPlanNodeId(oid_t plan_node_id);

  void SetExecutor(nstore::executor::AbstractExecutor* executor);

  inline nstore::executor::AbstractExecutor* GetExecutor() const {
    return executor;
  }

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual PlanNodeType GetPlanNodeType() const = 0;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Debugging convenience methods
  friend std::ostream& operator<<(std::ostream& os, const AbstractPlanNode& node);

  std::string debug() const;
  std::string debug(bool traverse) const;
  std::string debug(const std::string& spacer) const;
  virtual std::string debugInfo(const std::string& spacer) const = 0;

 private:

  // Every plan node will have a unique id assigned to it at compile time
  oid_t plan_node_id;

  // A node can have multiple children and parents
  std::vector<AbstractPlanNode*> children;
  std::vector<oid_t> children_ids;

  std::vector<AbstractPlanNode*> parents;
  std::vector<oid_t> parent_ids;

  // We also keep a pointer to this node's executor so that we can
  // reference it quickly at runtime without having to look-up a map
  nstore::executor::AbstractExecutor* executor = nullptr; // volatile

  // Some Executors can take advantage of multiple internal plan nodes
  // to perform tasks inline.
  std::map<PlanNodeType, AbstractPlanNode*> inlined_nodes;

  bool is_inlined = false;

};

} // namespace planner
} // namespace nstore
