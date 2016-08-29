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
#include "expression/constant_value_expression.h"

namespace peloton {
namespace planner {

IndexScanPlan::IndexScanPlan(storage::DataTable *table,
                             expression::AbstractExpression *predicate,
                             const std::vector<oid_t> &column_ids,
                             const IndexScanDesc &index_scan_desc)
    : index_(index_scan_desc.index_obj),
      column_ids_(column_ids),
      key_column_ids_(std::move(index_scan_desc.tuple_column_id_list)),
      expr_types_(std::move(index_scan_desc.expr_list)),
      values_with_params_(std::move(index_scan_desc.value_list)),
      runtime_keys_(std::move(index_scan_desc.runtime_key_list)),
      // Initialize the index scan predicate object and initialize all
      // keys that we could initialize
      index_predicate_() {

  LOG_TRACE("Creating an Index Scan Plan");

  SetTargetTable(table);

  if (predicate != NULL) {
    // we need to copy it here because eventually predicate will be destroyed by
    // its owner...
    predicate = predicate->Copy();
    ReplaceColumnExpressions(table->GetSchema(), predicate);
    predicate_with_params_ =
        std::unique_ptr<expression::AbstractExpression>(predicate);
    SetPredicate(predicate_with_params_->Copy());
  }

  // copy the value over for binding purpose
  values_ = values_with_params_;

  // Then add the only conjunction predicate into the index predicate list
  // (at least for now we only supports single conjunction)
  index_predicate_.AddConjunctionScanPredicate(index_.get(), values_,
                                               key_column_ids_, expr_types_);

  return;
}

void IndexScanPlan::SetParameterValues(std::vector<Value> *values) {
  LOG_TRACE("Setting parameter values in Index Scans");

  auto where = predicate_with_params_->Copy();
  expression::ExpressionUtil::ConvertParameterExpressions(
      where, values, GetTable()->GetSchema());
  SetPredicate(where);

  values_ = values_with_params_;
  for (unsigned int i = 0; i < values_.size(); ++i) {
    auto &value = values_[i];
    auto column_id = key_column_ids_[i];
    if (value.GetValueType() == VALUE_TYPE_PARAMETER_OFFSET) {
      value = values->at(ValuePeeker::PeekParameterOffset(value)).CastAs(
          GetTable()->GetSchema()->GetColumn(column_id).GetType());
    }
  }

  // Also bind values to index scan predicate object
  //
  // NOTE: This could only be called by one thread at a time
  index_predicate_.LateBindValues(index_.get(), *values);

  for (auto &child_plan : GetChildren()) {
    child_plan->SetParameterValues(values);
  }
}

}  // namespace planner
}  // namespace peloton
