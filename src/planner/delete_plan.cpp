//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_plan.cpp
//
// Identification: src/planner/delete_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/delete_plan.h"

#include "parser/delete_statement.h"
#include "catalog/catalog.h"
#include "expression/expression_util.h"
#include "storage/data_table.h"

namespace peloton {

namespace expression {
class TupleValueExpression;
}
namespace planner {

DeletePlan::DeletePlan(storage::DataTable *table, bool truncate)
    : target_table_(table), truncate_(truncate) {
  LOG_TRACE("Creating a Delete Plan");
}

// Creates the delete plan. The scan plan should be added outside
DeletePlan::DeletePlan(storage::DataTable *table,
                       const expression::AbstractExpression *predicate)
    : target_table_(table) {
  // if expr is null , delete all tuples from table
  if (predicate == nullptr) {
    LOG_TRACE("No expression, setting truncate to true");
    predicate = nullptr;
    truncate_ = true;
  } else {
    LOG_TRACE("Replacing COLUMN_REF with TupleValueExpressions");
    predicate_ = predicate->Copy();
    expression::ExpressionUtil::TransformExpression(target_table_->GetSchema(),
                                                    predicate_);
  }
}

void DeletePlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in Delete");
  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

}  // namespace planner
}  // namespace peloton
