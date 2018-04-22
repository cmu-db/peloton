//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_scan_plan.cpp
//
// Identification: src/planner/abstract_scan_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/abstract_scan_plan.h"

#include "storage/data_table.h"

namespace peloton {
namespace planner {

void AbstractScan::PerformBinding(BindingContext &binding_context) {
  const auto &children = GetChildren();
  if (children.size() > 0) {
    // We're scanning some intermediate result. This scan node doesn't produce
    // any new attributes, and hence, doesn't need to worry about binding its
    // column IDs to any attributes
    for (auto &child : GetChildren()) {
      child->PerformBinding(binding_context);
    }
  } else {
    // We're scanning a table

    // Fill up the column IDs if empty
    if (GetColumnIds().size() == 0) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }

    // Collect _all_ table columns
    const auto *schema = GetTable()->GetSchema();
    for (oid_t col_id = 0; col_id < schema->GetColumnCount(); col_id++) {
      const auto column = schema->GetColumn(col_id);
      bool nullable = schema->AllowNull(col_id);
      auto type = codegen::type::Type{column.GetType(), nullable};
      attributes_.push_back(AttributeInfo{type, col_id, column.GetName()});
    }

    // Perform attribute binding only for actual output columns
    const auto &input_col_ids = GetColumnIds();
    for (oid_t col_id = 0; col_id < input_col_ids.size(); col_id++) {
      const auto &ai = attributes_[input_col_ids[col_id]];
      LOG_TRACE("Attribute '%s.%s' (%u) binds to AI %p",
                GetTable()->GetName().c_str(), ai.name.c_str(), col_id, &ai);
      binding_context.BindNew(col_id, &ai);
    }
  }

  auto predicate = predicate_.get();
  if (predicate != nullptr) {
    // We build a new binding context because the attributes the predicate needs
    // may not be part of the scan's output. Hence, we create a new context that
    // includes _all_ the table's attributes.
    BindingContext all_cols_context;
    const auto *schema = GetTable()->GetSchema();
    for (oid_t col_id = 0; col_id < schema->GetColumnCount(); col_id++) {
      all_cols_context.BindNew(col_id, &attributes_[col_id]);
    }

    predicate->PerformBinding({&all_cols_context});
  }
}

}  // namespace planner
}  // namespace peloton
