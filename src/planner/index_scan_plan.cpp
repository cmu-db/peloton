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
#include "expression/expression_util.h"

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

void IndexScanPlan::SetParameterValues(std::vector<Value> *values) {
  LOG_INFO("Setting parameter values in Index Scans");
  auto where = GetPredicate()->Copy();
  expression::ExpressionUtil::ConvertParameterExpressions(where, values, GetTable()->GetSchema());
  SetPredicate(where);
  for (auto &value : values_) {
    if (value.GetValueType() == VALUE_TYPE_FOR_BINDING_ONLY_INTEGER) {
      value = values->at(ValuePeeker::PeekBindingOnlyInteger(value));
    }
  }

  for (auto &child_plan : GetChildren()) {
    child_plan->SetParameterValues(values);
  }
}

}  // namespace planner
}  // namespace peloton
