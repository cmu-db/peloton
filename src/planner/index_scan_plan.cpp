//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_plan.cpp
//
// Identification: src/planner/index_scan_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/index_scan_plan.h"
#include "storage/data_table.h"
#include "common/types.h"

namespace peloton {
namespace planner {

IndexScanPlan::IndexScanPlan(storage::DataTable *table,
                             expression::AbstractExpression *predicate,
                             const std::vector<oid_t> &column_ids,
                             const IndexScanDesc &index_scan_desc)
    : index_(index_scan_desc.index),
      column_ids_(column_ids),
      key_column_ids_(std::move(index_scan_desc.key_column_ids)),
      expr_types_(std::move(index_scan_desc.expr_types)),
      values_(std::move(index_scan_desc.values)),
      runtime_keys_(std::move(index_scan_desc.runtime_keys)) {

  SetTargetTable(table);

  if (predicate != NULL) {
    // we need to copy it here because eventually predicate will be destroyed by
    // its owner...
    auto where = predicate->Copy();
    ReplaceColumnExpressions(table->GetSchema(), where);
    SetPredicate(where);
  }
}

}  // namespace planner
}  // namespace peloton
