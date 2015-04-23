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

#include "catalog/database.h"
#include "executor/abstract_executor.h"
#include "common/types.h"
#include <json_spirit.h>

namespace nstore {
namespace planner {

class PlanColumn;
class LogicalTile;
class LogicalTile;

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

  void SetExecutor(executor::AbstractExecutor* executor);

  inline executor::AbstractExecutor* GetExecutor() const {
    return executor;
  }

  void SetInputs(const std::vector<LogicalTile*> &val);

  std::vector<LogicalTile*>& GetInputs();

  void SetOutput(LogicalTile* val);

  LogicalTile *GetOutput() const;

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual PlanNodeType GetPlanNodeType() const = 0;

  std::vector<oid_t> GetOutputColumnGuids() const;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  virtual AbstractPlanNode* LoadFromJSONObject(json_spirit::Object& obj,
                                           const catalog::Database* catalog_db);

  virtual int GetColumnIndexFromGuid(int guid,
                                     const catalog::Database* db) const;

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
  executor::AbstractExecutor* executor; // volatile

  // Output tile group
  // This is where we will write the results of the plan node's execution
  LogicalTile* output;

  // Input tile groups
  // These tile groups are derived from the output of this node's children
  std::vector<LogicalTile*> inputs;

  // Some Executors can take advantage of multiple internal PlanNodes
  // to perform tasks inline. This can be a big speed increase
  std::map<PlanNodeType, AbstractPlanNode*> inlined_nodes;

  bool is_inlined;

  std::vector<oid_t> output_column_guids;

};

} // namespace planner
} // namespace nstore
