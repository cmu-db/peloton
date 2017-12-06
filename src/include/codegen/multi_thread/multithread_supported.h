//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// multithread_supported.h
//
// Identification: src/include/codegen/multi_thread/multithread_supported.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

namespace peloton {
namespace codegen {

bool MultithreadSupported(const planner::AbstractPlan &plan);

}  // namespace codegen
}  // namespace peloton
