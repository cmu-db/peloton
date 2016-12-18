
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// order_by_plan.cpp
//
// Identification: /peloton/src/planner/order_by_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
 
#include <memory>
#include <string>
#include <vector>

#include "planner/order_by_plan.h"

#include "type/types.h"
#include "expression/abstract_expression.h"


namespace peloton {
namespace planner {

	OrderByPlan::OrderByPlan(const std::vector<oid_t> &sort_keys,
             const std::vector<bool> &descend_flags,
             const std::vector<oid_t> &output_column_ids)
     : sort_keys_(sort_keys),
       descend_flags_(descend_flags),
       output_column_ids_(output_column_ids) {}
}
}


