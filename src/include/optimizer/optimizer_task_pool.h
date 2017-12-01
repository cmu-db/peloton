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

namespace peloton {
namespace optimizer {
//===--------------------------------------------------------------------===//
// Base task pool class
//===--------------------------------------------------------------------===//
class OptimizerTaskPool {
 public:
  virtual std::unique_ptr<OptimizerTask> Pop() = 0;
  virtual void Push(OptimizerTask* task) = 0;
  virtual bool Empty() = 0;
};


//===--------------------------------------------------------------------===//
// Task stack
//===--------------------------------------------------------------------===//
class OptimizerTaskStack : public OptimizerTaskPool {
 public:
  virtual std::unique_ptr<OptimizerTask> Pop() {
    auto task = task_stack_.top();
    task_stack_.pop();
    return task;
  }

  virtual void Push(OptimizerTask* task)  {
    task_stack_.push(std::unique_ptr<OptimizerTask>(task));
  }

  virtual bool Empty() {
    return task_stack_.empty();
  }

 private:
  std::stack<std::unique_ptr<OptimizerTask>> task_stack_;

};

} // namespace optimizer
} // namespace peloton