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

#include "property_set.h"

namespace peloton {
namespace optimizer {

class OptimizeContext {
 public:
  OptimizeContext(
      std::unique_ptr<PropertySet> required_prop,
      double cost_upper_bound = -1) :
      required_prop(std::move(required_prop)), cost_upper_bound(cost_upper_bound) {}
  std::unique_ptr<PropertySet> required_prop;
  double cost_upper_bound;
};


} // namespace optimizer
} // namespace peloton