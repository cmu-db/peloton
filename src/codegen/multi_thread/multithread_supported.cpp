//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task_info.cpp
//
// Identification: src/codegen/multi_thread/multithread_supported.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/multi_thread/multithread_supported.h"

namespace peloton {
namespace codegen {

bool MultithreadSupported(const planner::AbstractPlan &plan) {
  switch (plan.GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN:
      return true;
    default:
      return false;
  }
}

}  // namespace codegen
}  // namespace peloton
