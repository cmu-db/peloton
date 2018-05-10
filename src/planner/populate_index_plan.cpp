
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// populate_index_plan.cpp
//
// Identification: /peloton/src/planner/populate_index_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/populate_index_plan.h"

#include "common/internal_types.h"
#include "expression/abstract_expression.h"

namespace peloton {
namespace planner {
PopulateIndexPlan::PopulateIndexPlan(storage::DataTable *table,
                                     std::vector<oid_t> column_ids, std::string index_name, bool concurrent)
    : target_table_(table), column_ids_(column_ids), index_name_(index_name), concurrent_(concurrent) {}
}
}
