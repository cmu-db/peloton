//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/optimizer/optimizer_task_pool.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/optimizer_task.h"
#include <stack>
#include <memory>

namespace peloton {
namespace optimizer {
/**
 * @brief The base class of a task pool, which needs to support adding tasks and
 *  getting available tasks from the pool. Note that a single-threaded task pool
 *  is identical to a stack but we may need to implement a different data
 *  structure for multi-threaded optimization
 */

template <class Node, class OperatorType, class OperatorExpr>
class OptimizerTaskPool {
 public:
  virtual std::unique_ptr<OptimizerTask<Node, OperatorType, OperatorExpr>> Pop() = 0;
  virtual void Push(OptimizerTask<Node,OperatorType,OperatorExpr> *task) = 0;
  virtual bool Empty() = 0;
};

/**
 * @brief Stack implementation of the task pool
 */
template <class Node, class OperatorType, class OperatorExpr>
class OptimizerTaskStack : public OptimizerTaskPool<Node,OperatorType,OperatorExpr> {
 public:
  virtual std::unique_ptr<OptimizerTask<Node, OperatorType, OperatorExpr>> Pop() {
    auto task = std::move(task_stack_.top());
    task_stack_.pop();
    return task;
  }

  virtual void Push(OptimizerTask<Node, OperatorType, OperatorExpr> *task) {
    task_stack_.push(std::unique_ptr<OptimizerTask<Node,OperatorType,OperatorExpr>>(task));
  }

  virtual bool Empty() { return task_stack_.empty(); }

 private:
  std::stack<std::unique_ptr<OptimizerTask<Node,OperatorType,OperatorExpr>>> task_stack_;
};

}  // namespace optimizer
}  // namespace peloton
