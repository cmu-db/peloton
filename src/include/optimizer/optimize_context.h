//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/include/optimizer/optimize_context.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/property_set.h"
#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_metadata.h"

namespace peloton {
namespace optimizer {

class OptimizeContext {
 public:
  OptimizeContext(
      OptimizerTaskPool *task_pool, OptimizerMetadata *metadata,
      std::unique_ptr<PropertySet> required_prop,
      double cost_upper_bound = std::numeric_limits<double>::max()) :
      task_pool(task_pool), metadata(metadata),
      required_prop(std::move(required_prop)), cost_upper_bound(cost_upper_bound) {}

  OptimizerTaskPool *task_pool;
  OptimizerMetadata *metadata;
  std::unique_ptr<PropertySet> required_prop;
  double cost_upper_bound;
};


} // namespace optimizer
} // namespace peloton