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

#include <memory>
#include <map>

#include "common/timer.h"
#include "common/internal_types.h"

#include "expression/abstract_expression.h"
#include "optimizer/property_set.h"
#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_task_pool.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace optimizer {

class OptimizerMetadata;

class OptimizeContext {
 public:
  OptimizeContext(OptimizerMetadata *metadata,
                  std::shared_ptr<PropertySet> required_prop,
                  double cost_upper_bound = std::numeric_limits<double>::max())
      : metadata(metadata),
        required_prop(required_prop),
        cost_upper_bound(cost_upper_bound) {}

  OptimizerMetadata *metadata;
  std::shared_ptr<PropertySet> required_prop;
  double cost_upper_bound;
  std::map<PredicateInfo, std::vector<expression::AbstractExpression *>> transitive_table;
};

}  // namespace optimizer
}  // namespace peloton
