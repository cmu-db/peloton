//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_executor.h
//
// Identification: src/include/executor/abstract_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include <type/value.h>

#include "executor/logical_tile.h"

namespace peloton {

namespace planner {
class AbstractPlan;
}

namespace executor {
class ExecutorContext;
}

namespace executor {

class AbstractExecutor {
 public:
  AbstractExecutor(const AbstractExecutor &) = delete;
  AbstractExecutor &operator=(const AbstractExecutor &) = delete;
  AbstractExecutor(AbstractExecutor &&) = delete;
  AbstractExecutor &operator=(AbstractExecutor &&) = delete;

  virtual ~AbstractExecutor() {}

  bool Init();

  bool Execute();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(AbstractExecutor *child);

  const std::vector<AbstractExecutor *> &GetChildren() const;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Virtual because we want to be able to intercept via the mock executor
  // in test cases.
  virtual LogicalTile *GetOutput();

  const planner::AbstractPlan *GetRawNode() const { return node_; }

  // set the context
  void SetContext(type::Value &value);

  // clear the context
  void ClearContext();

  // Update the predicate in runtime. This is used in Nested Loop Join. Since
  // some executor do not need this function, we set it to empty function.
  virtual void UpdatePredicate(const std::vector<oid_t> &key_column_ids
                                   UNUSED_ATTRIBUTE,
                               const std::vector<type::Value> &values
                                   UNUSED_ATTRIBUTE) {}

  // Used to reset the state. For now it's overloaded by index scan executor
  virtual void ResetState() {}

 protected:
  // NOTE: The reason why we keep the plan node separate from the executor
  // context is because we might want to reuse the plan multiple times
  // with different executor contexts
  explicit AbstractExecutor(const planner::AbstractPlan *node,
                            ExecutorContext *executor_context);

  /** @brief Init function to be overriden by derived class. */
  virtual bool DInit() = 0;

  /** @brief Workhorse function to be overriden by derived class. */
  virtual bool DExecute() = 0;

  void SetOutput(LogicalTile *val);

  /**
   * @brief Convenience method to return plan node corresponding to this
   *        executor, appropriately type-casted.
   *
   * @return Reference to plan node.
   */
  template <class T>
  inline const T &GetPlanNode() {
    const T *node = dynamic_cast<const T *>(node_);
    PL_ASSERT(node);
    return *node;
  }

  /** @brief Children nodes of this executor in the executor tree. */
  std::vector<AbstractExecutor *> children_;

 private:
  // Output logical tile
  // This is where we will write the results of the plan node's execution
  std::unique_ptr<LogicalTile> output;

  /** @brief Plan node corresponding to this executor. */
  const planner::AbstractPlan *node_ = nullptr;

 protected:
  // Executor context
  ExecutorContext *executor_context_ = nullptr;
};

}  // namespace executor
}  // namespace peloton
