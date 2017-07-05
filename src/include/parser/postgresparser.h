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
  //===--------------------------------------------------------------------===//
  // Helper Functions
  //===--------------------------------------------------------------------===//

  // helper for c_str copy
  static char* cstrdup(const char* c_str) {
    char* new_str = new char[strlen(c_str) + 1];
    strcpy(new_str, c_str);
    return new_str;
  }

  static ColumnDefinition::FKConstrActionType CharToActionType(char &type) {
    switch (type) {
      case 'a':return ColumnDefinition::FKConstrActionType::NOACTION;
      case 'r':return ColumnDefinition::FKConstrActionType::RESTRICT;
      case 'c':return ColumnDefinition::FKConstrActionType::CASCADE;
      case 'n':return ColumnDefinition::FKConstrActionType::SETNULL;
      case 'd':return ColumnDefinition::FKConstrActionType::SETDEFAULT;
      default:return ColumnDefinition::FKConstrActionType::NOACTION;
    }
  }

  static ColumnDefinition::FKConstrMatchType CharToMatchType(char &type) {
    switch (type) {
      case 'f':return ColumnDefinition::FKConstrMatchType::FULL;
      case 'p':return ColumnDefinition::FKConstrMatchType::PARTIAL;
      case 's':return ColumnDefinition::FKConstrMatchType::SIMPLE;
      default:return ColumnDefinition::FKConstrMatchType::SIMPLE;
    }
  }

  static bool IsAggregateFunction(std::string& fun_name) {
    if (fun_name == "min" || fun_name == "max" ||
        fun_name == "count" || fun_name == "avg" ||
        fun_name == "sum")
      return true;
    return false;
  }

  //===--------------------------------------------------------------------===//
  // Transform Functions
  //===--------------------------------------------------------------------===//

  // transform helper for internal parse tree
  static parser::SQLStatementList*
    PgQueryInternalParsetreeTransform(PgQueryInternalParsetreeAndError stmt);

  // transform helper for Alias parsenodes
  static char* AliasTransform(Alias* root);

  // transform helper for RangeVar parsenodes
  static parser::TableRef* RangeVarTransform(RangeVar* root);

  // transform helper for RangeSubselect parsenodes
  static parser::TableRef* RangeSubselectTransform(RangeSubselect* root);

  // transform helper for JoinExpr parsenodes
  static parser::JoinDefinition* JoinTransform(JoinExpr* root);

  // transform helper for from clauses
  static parser::TableRef* FromTransform(SelectStmt* root);

  // transform helper for select targets
  static std::vector<expression::AbstractExpression*>* TargetTransform(List* root);

  // transform helper for all expr nodes
  static expression::AbstractExpression* ExprTransform(Node* root);

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

  // transform helper for parameter refs
  static expression::AbstractExpression* ParamRefTransform(ParamRef* root);

  // transform helper for expressions
  static expression::AbstractExpression* ExprTransform(Expr* root);

  // transform helper for case expressions
  static expression::AbstractExpression* CaseExprTransform(CaseExpr* root);

  // transform helper for group by clauses
  static parser::GroupByDescription* GroupByTransform(List* root, Node* having);

  // transform helper for order by clauses
  static parser::OrderDescription* OrderByTransform(List* order);

  // transform helper for table column definitions
  static parser::ColumnDefinition* ColumnDefTransform(ColumnDef *root);

  // transform helper for create statements
  static parser::SQLStatement* CreateTransform(CreateStmt* root);

  // transform helper for create index statements
  static parser::SQLStatement* CreateIndexTransform(IndexStmt* root);

  // transform helper for create db statement
  static parser::SQLStatement* CreateDbTransform(CreatedbStmt* root);

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
  static parser::SQLStatement* NodeTransform(Node* stmt);

  // transform helper for the whole parse list
  static parser::SQLStatementList* ListTransform(List* root);

  // transform helper for update statement
  static parser::UpdateStatement* UpdateTransform(UpdateStmt* update_stmt);

  // transform helper for update statement
  static std::vector<parser::UpdateClause*>* UpdateTargetTransform(List* root);

  // transform helper for drop statement
  static parser::DropStatement* DropTransform(DropStmt* root);

  // transform helper for truncate statement
  static parser::DeleteStatement* TruncateTransform(TruncateStmt* root);

  // transform helper for transaction statement
  static parser::TransactionStatement* TransactionTransform(TransactionStmt* root);

  // transform helper for execute statement
  static parser::ExecuteStatement* ExecuteTransform(ExecuteStmt* root);

  // transform helper for constant values
  static expression::AbstractExpression* ValueTransform(value val);

  static std::vector<expression::AbstractExpression*>* ParamListTransform(List* root);

  // transform helper for execute statement
  static parser::PrepareStatement* PrepareTransform(PrepareStmt* root);

  // transform helper for execute statement
  static parser::CopyStatement* CopyTransform(CopyStmt* root);

  // transform helper for analyze statement
  static parser::AnalyzeStatement* VacuumTransform(VacuumStmt* root);
};

}  // End parser namespace
}  // End peloton namespace
