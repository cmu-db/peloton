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

#include "common/timer.h"

#include "optimizer/property_set.h"
#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_task_pool.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace optimizer {

template <class Operator, class OperatorType, class OperatorExpr>
class OptimizerMetadata;

template <class Operator, class OperatorType, class OperatorExpr>
class OptimizeContext {
 public:
  OptimizeContext(OptimizerMetadata<Operator,OperatorType,OperatorExpr> *metadata,
                  std::shared_ptr<PropertySet> required_prop,
                  double cost_upper_bound = std::numeric_limits<double>::max())
      : metadata(metadata),
        required_prop(required_prop),
        cost_upper_bound(cost_upper_bound) {}

  OptimizerMetadata<Operator,OperatorType,OperatorExpr> *metadata;
  std::shared_ptr<PropertySet> required_prop;
  double cost_upper_bound;
};

}  // namespace optimizer
}  // namespace peloton
