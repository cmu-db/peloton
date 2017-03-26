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
  static parser::SQLStatementList* ParseSQLString(const char* sql);
  static parser::SQLStatementList* ParseSQLString(
      const std::string& sql);

  static PostgresParser& GetInstance();

  std::unique_ptr<parser::SQLStatementList> BuildParseTree(
      const std::string& query_string);

 private:
  static parser::SQLStatementList*
    PgQueryInternalParsetreeTransform(PgQueryInternalParsetreeAndError stmt);

  // transform helper for Alias parsenodes
  static char* AliasTransform(Alias* root);

  // transform helper for RangeVar parsenodes
  static parser::TableRef* RangeVarTransform(RangeVar* root);

  // transform helper for JoinExpr parsenodes
  static parser::JoinDefinition* JoinTransform(JoinExpr* root);

  // transform helper for from clauses
  static parser::TableRef* FromTransform(List* root);

  // transform helper for select targets
  static std::vector<expression::AbstractExpression*>* TargetTransform(List* root);

  // transform helper for A_Expr nodes
  static expression::AbstractExpression* AExprTransform(A_Expr* root);

  // transform helper for BoolExpr nodes
  static expression::AbstractExpression* BoolExprTransform(BoolExpr* root);

  // transform helper for where clauses
  static expression::AbstractExpression* WhereTransform(Node* root);

  // transform helper for column refs
  static expression::AbstractExpression* ColumnRefTransform(ColumnRef* root);

  // transform helper for constant values
  static expression::AbstractExpression* ConstTransform(A_Const* root);

  // transform helper for function calls
  static expression::AbstractExpression* FuncCallTransform(FuncCall* root);

  // transform helper for group by clauses
  static parser::GroupByDescription* GroupByTransform(List* root, Node* having);

  // transform helper for order by clauses
  static parser::OrderDescription* OrderByTransform(List* order);

  // transform helper for table column definitions
  static parser::ColumnDefinition* ColumnDefTransform(ColumnDef *root);

  // transform helper for create statements
  static parser::SQLStatement* CreateTransform(CreateStmt* root);

  // transform helper for column name (for insert statement)
  static std::vector<char*>* ColumnNameTransform(List* root);

  // transform helper for ListsTransform (insert multiple rows)
  static std::vector<std::vector<expression::AbstractExpression*>*>*
      ValueListsTransform(List* root);

  // transform helper for insert statements
  static parser::SQLStatement* InsertTransform(InsertStmt* root);

  // transform helper for select statements
  static parser::SQLStatement* SelectTransform(SelectStmt* root);

  // transform helper for delete statements
  static parser::SQLStatement* DeleteTransform(DeleteStmt* root);

  // transform helper for single node in parse list
  static parser::SQLStatement* NodeTransform(ListCell* stmt);

  // transform helper for the whole parse list
  static parser::SQLStatementList* ListTransform(List* root);

  // transform helper for update statement
  static parser::UpdateStatement* UpdateTransform(UpdateStmt* update_stmt);

  // transform helper for update statement
  static std::vector<parser::UpdateClause*>* UpdateTargetTransform(List* root);
};

}  // End parser namespace
}  // End peloton namespace
