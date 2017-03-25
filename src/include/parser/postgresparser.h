//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgresparser.h
//
// Identification: src/include/parser/postgresparser.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statements.h"
#include "parser/pg_query.h"
#include "parser/parser.h"
#include "parser/parsenodes.h"

namespace peloton {
namespace parser {

//===--------------------------------------------------------------------===//
// PostgresParser
//
// This is the parser that gets a Postgres parse tree first and then
// transform into a Peloton parse tree.
// To start working on PostgresParser:
// First get familiar with List and ListCell structures defined in
// pg_list.h, then take a look at the Postgres types defined in nodes.h.
//
// To add support for a new type of Statement:
// Find corresponding parsenode in
// /third_party/libpg_query/src/postgres/include/nodes/parsenodes.h or
// /third_party/libpg_query/src/postgres/include/nodes/primnodes.h and
// copy to src/include/parser/parsenodes.h, then add a helper function
// for the statement.
//
//===--------------------------------------------------------------------===//

class PostgresParser {
 public:
  PostgresParser();
  ~PostgresParser();

  // Parse a given query
  static PgQueryInternalParsetreeAndError ParseSQLString(const char* sql);
  static PgQueryInternalParsetreeAndError ParseSQLString(
      const std::string& sql);

  static PostgresParser& GetInstance();

  std::unique_ptr<parser::SQLStatementList> BuildParseTree(
      const std::string& query_string);

 private:
  // transform helper for Alias parsenodes
  char* AliasTransform(Alias* root);

  // transform helper for RangeVar parsenodes
  parser::TableRef* RangeVarTransform(RangeVar* root);

  // transform helper for JoinExpr parsenodes
  parser::JoinDefinition* JoinTransform(JoinExpr* root);

  // transform helper for from clauses
  parser::TableRef* FromTransform(List* root);

  // transform helper for select targets
  std::vector<expression::AbstractExpression*>* TargetTransform(List* root);

  // transform helper for A_Expr nodes
  expression::AbstractExpression* AExprTransform(A_Expr* root);

  // transform helper for BoolExpr nodes
  expression::AbstractExpression* BoolExprTransform(BoolExpr* root);

  // transform helper for where clauses
  expression::AbstractExpression* WhereTransform(Node* root);

  // transform helper for column refs
  expression::AbstractExpression* ColumnRefTransform(ColumnRef* root);

  // transform helper for constant values
  expression::AbstractExpression* ConstTransform(A_Const* root);

  // transform helper for function calls
  expression::AbstractExpression* FuncCallTransform(FuncCall* root);

  // transform helper for group by clauses
  parser::GroupByDescription* GroupByTransform(List* root, Node* having);

  // transform helper for order by clauses
  parser::OrderDescription* OrderByTransform(List* order);

  // transform helper for table column definitions
  parser::ColumnDefinition* ColumnDefTransform(ColumnDef *root);

  // transform helper for create statements
  parser::SQLStatement* CreateTransform(CreateStmt* root);

  // transform helper for column name (for insert statement)
  std::vector<char*>* ColumnNameTransform(List* root);

  // transform helper for ListsTransform (insert multiple rows)
  std::vector<std::vector<expression::AbstractExpression*>*>*
      ValueListsTransform(List* root);

  // transform helper for insert statements
  parser::SQLStatement* InsertTransform(InsertStmt* root);

  // transform helper for select statements
  parser::SQLStatement* SelectTransform(SelectStmt* root);

  // transform helper for delete statements
  parser::SQLStatement* DeleteTransform(DeleteStmt* root);

  // transform helper for single node in parse list
  parser::SQLStatement* NodeTransform(ListCell* stmt);

  // transform helper for the whole parse list
  std::unique_ptr<parser::SQLStatementList> ListTransform(List* root);

  // transform helper for update statement
  parser::UpdateStatement* UpdateTransform(UpdateStmt* update_stmt);

  // transform helper for update statement
  std::vector<parser::UpdateClause*>* UpdateTargetTransform(List* root);
};

}  // End parser namespace
}  // End peloton namespace
