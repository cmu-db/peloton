//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_plan.cpp
//
// Identification: src/planner/copy_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/copy_plan.h"
#include "storage/data_table.h"
#include "catalog/manager.h"
#include "common/types.h"
#include "common/macros.h"
#include "common/logger.h"
#include "expression/expression_util.h"
#include "catalog/schema.h"
#include "catalog/catalog.h"

#include "parser/statement_copy.h"

namespace peloton {
namespace planner {

CopyPlan::CopyPlan(parser::CopyStatement *select_node) {
  LOG_DEBUG("Creating a Sequential Scan Plan");
  (void)select_node;
  //  auto target_table = static_cast<storage::DataTable *>(
  //      catalog::Catalog::GetInstance()->GetTableWithName(
  //          select_node->from_table->GetDatabaseName(),
  //          select_node->from_table->GetTableName()));
  //  SetTargetTable(target_table);
  //  ColumnIds().clear();
  //  // Check if there is an aggregate function in query
  //  bool function_found = false;
  //  for (auto elem : *select_node->select_list) {
  //    if (elem->GetExpressionType() == EXPRESSION_TYPE_FUNCTION_REF) {
  //      function_found = true;
  //      break;
  //    }
  //  }
  //  // Pass all columns
  //  // TODO: This isn't efficient. Needs to be fixed
  //  if (function_found) {
  //    auto &schema_columns = GetTable()->GetSchema()->GetColumns();
  //    for (auto column : schema_columns) {
  //      oid_t col_id = CopyPlan::GetColumnID(column.column_name);
  //      SetColumnId(col_id);
  //    }
  //  }
  //  // Pass columns in select_list
  //  else {
  //    if (select_node->select_list->at(0)->GetExpressionType() !=
  //        EXPRESSION_TYPE_STAR) {
  //      for (auto col : *select_node->select_list) {
  //        LOG_TRACE("ExpressionType: %s",
  //                  ExpressionTypeToString(col->GetExpressionType()).c_str());
  //        auto col_name = col->GetName();
  //        oid_t col_id = CopyPlan::GetColumnID(std::string(col_name));
  //        SetColumnId(col_id);
  //      }
  //    } else {
  //      auto allColumns = GetTable()->GetSchema()->GetColumns();
  //      for (uint i = 0; i < allColumns.size(); i++) SetColumnId(i);
  //    }
  //  }
  //
  //  // Check for "For Update" flag
  //  if (select_node->is_for_update == true) {
  //    SetForUpdateFlag(true);
  //  }
  //
  //  // Keep a copy of the where clause to be binded to values
  //  if (select_node->where_clause != NULL) {
  //    auto predicate = select_node->where_clause->Copy();
  //    // Replace COLUMN_REF expressions with TupleValue expressions
  //    expression::ExpressionUtil::ReplaceColumnExpressions(
  //        GetTable()->GetSchema(), predicate);
  //    predicate_with_params_ =
  //        std::unique_ptr<expression::AbstractExpression>(predicate->Copy());
  //    SetPredicate(predicate);
  //  }
}

}  // namespace planner
}  // namespace peloton
