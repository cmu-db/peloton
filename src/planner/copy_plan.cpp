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

CopyPlan::CopyPlan(parser::CopyStatement* copy_parse_tree)
    : file_path_(copy_parse_tree->GetFilePath()) {
  LOG_DEBUG("Creating a Copy Plan");

  // Hard code star expression
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
  table_ref->table_name = copy_parse_tree->table_name;

  // Construct select stmt
  auto select_stmt = new parser::SelectStatement();
  select_stmt->from_table = table_ref;
  select_stmt->select_list = select_list;

  std::unique_ptr<planner::SeqScanPlan> child_SelectPlan(
      new planner::SeqScanPlan(select_stmt));
  LOG_TRACE("Sequential scan plan created");

  AddChild(std::move(child_SelectPlan));
}

}  // namespace planner
}  // namespace peloton
