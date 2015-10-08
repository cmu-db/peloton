//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_executor.h
//
// Identification: src/backend/executor/abstract_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <memory>
#include <vector>

#include "../planner/abstract_plan.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/logical_tile.h"

namespace peloton {
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
    assert(node);
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
