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

/**
 * XXX Copy plan by default assumes a seq scan plan node as it's child
 * This can be extended when we support nested queries in COPY query
 */
CopyPlan::CopyPlan(parser::CopyStatement* copy_parse_tree)
    : file_path(copy_parse_tree->file_path) {
  LOG_DEBUG("Creating a Copy Plan");

  // Hard code star expression :(
  char* star = new char[2];
  strcpy(star, "*");
  auto star_expr = new peloton::expression::ParserExpression(
      peloton::EXPRESSION_TYPE_STAR, star);

  // Push star expression to list
  auto select_list =
      new std::vector<peloton::expression::AbstractExpression*>();
  select_list->push_back(star_expr);

  // Populate seq scan table ref
  parser::TableRef* table_ref =
      new parser::TableRef(peloton::TABLE_REFERENCE_TYPE_NAME);
  table_ref->table_name = static_cast<expression::ParserExpression*>(
      copy_parse_tree->table_name->Copy());

  // Construct select stmt
  std::unique_ptr<parser::SelectStatement> select_stmt(
      new parser::SelectStatement());
  select_stmt->from_table = table_ref;
  select_stmt->select_list = select_list;

  // Add the child seq scan plan
  std::unique_ptr<planner::SeqScanPlan> child_SelectPlan(
      new planner::SeqScanPlan(select_stmt.get()));
  LOG_TRACE("Sequential scan plan for copy created");
  AddChild(std::move(child_SelectPlan));

  // If we're copying the query metric table, then we need to handle the
  // deserialization of prepared stmt parameters
  if (std::string(table_ref->table_name->GetName()) == QUERY_METRIC_NAME) {
    LOG_DEBUG("Copying the query_metric table.");
    deserialize_parameters = true;
  }
}
}  // namespace planner
}  // namespace peloton
