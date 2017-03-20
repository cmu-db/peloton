//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgresparser.cpp
//
// Identification: src/parser/postgresparser.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>

#include "parser/postgresparser.h"
#include "parser/select_statement.h"
#include "common/exception.h"
#include "expression/star_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/function_expression.h"
#include "expression/conjunction_expression.h"
#include "type/types.h"
#include "type/value_factory.h"

namespace peloton {
namespace parser {

PostgresParser::PostgresParser() {}

PostgresParser::~PostgresParser() {}

// This function takes a Postgres Alias parsenode and extracts the name of
// the alias and return a duplicate of the string.
char* PostgresParser::AliasTransform(Alias* root) {
  if (root == NULL) {
    return NULL;
  }

  return strdup(root->aliasname);
}

// This function takes a Postgres JoinExpr parsenode and transfers it to a
// Peloton JoinDefinition object. Depends on AExprTransform and
// BoolExprTransform.
parser::JoinDefinition* PostgresParser::JoinTransform(JoinExpr* root) {
  parser::JoinDefinition* result = NULL;
  // Natrual join is not supported
  if ((root->jointype > 4) || (root->isNatural)) {
    return NULL;
  }
  LOG_TRACE("Join type is %d\n", root->jointype);
  result = new parser::JoinDefinition();
  switch (root->jointype) {
    case JOIN_INNER: {
      result->type = StringToJoinType("inner");
      break;
    }
    case JOIN_LEFT: {
      result->type = StringToJoinType("left");
      break;
    }
    case JOIN_FULL: {
      result->type = StringToJoinType("outer");
      break;
    }
    case JOIN_RIGHT: {
      result->type = StringToJoinType("right");
      break;
    }
    case JOIN_SEMI: {
      result->type = StringToJoinType("semi");
      break;
    }
    default: {
      LOG_ERROR("Join type %d not supported yet...", root->jointype);
      delete result;
      return NULL;
    }
  }

  // Check the type of left arg and right arg before transform
  if (root->larg->type == T_RangeVar) {
    result->left = RangeVarTransform((RangeVar*)(root->larg));
  } else {
    LOG_ERROR("Join arg type %d not supported yet...\n", root->larg->type);
    delete result;
    return NULL;
  }
  if (root->rarg->type == T_RangeVar) {
    result->right = RangeVarTransform((RangeVar*)(root->rarg));
  } else {
    LOG_ERROR("Join arg type %d not supported yet...\n", root->larg->type);
    delete result;
    return NULL;
  }

  // transform the quals, depends on AExprTranform and BoolExprTransform
  switch (root->quals->type) {
    case T_A_Expr: {
      result->condition = AExprTransform((A_Expr*)(root->quals));
      break;
    }
    case T_BoolExpr: {
      result->condition = BoolExprTransform((BoolExpr*)(root->quals));
    }
    default: {
      LOG_ERROR("Join quals type %d not supported yet...\n", root->larg->type);
      delete result;
      return NULL;
    }
  }
  return result;
}

// This function takes in a single Postgres RangeVar parsenode and transfer
// it into a Peloton TableRef object.
parser::TableRef* PostgresParser::RangeVarTransform(RangeVar* root) {
  parser::TableRef* result =
      new parser::TableRef(StringToTableReferenceType("name"));

  if (root->schemaname) {
    result->schema = strdup(root->schemaname);
  }

  // parse alias
  result->alias = AliasTransform(root->alias);

  result->table_info_ = new parser::TableInfo();

  if (root->relname) {
    result->table_info_->table_name = strdup(root->relname);
  }

  if (root->catalogname) {
    result->table_info_->database_name = strdup(root->catalogname);
  }
  return result;
}

// This fucntion takes in fromClause of a Postgres SelectStmt and transfers
// into a Peloton TableRef object.
// TODO: support select from multiple sources, nested queries, various joins
parser::TableRef* PostgresParser::FromTransform(List* root) {
  // now support select from only one sources
  parser::TableRef* result = NULL;
  if (root->length > 1) {
    LOG_ERROR("Multiple from not handled yet\n");
    return result;
  }

  Node* node = (Node*)(root->head->data.ptr_value);
  switch (node->type) {
    case T_RangeVar: {
      result = RangeVarTransform((RangeVar*)node);
      break;
    }
    case T_JoinExpr: {
      result = new parser::TableRef(StringToTableReferenceType("join"));
      result->join = JoinTransform((JoinExpr*)node);
      if (result->join == NULL) {
        delete result;
        result = NULL;
      }
      break;
    }
    case T_SelectStmt: {
      result = new parser::TableRef(StringToTableReferenceType("select"));
      result->select =
          (parser::SelectStatement*)SelectTransform((SelectStmt*)node);
      if (result->select == NULL) {
        delete result;
        result = NULL;
      }
      break;
    }
    default: { LOG_ERROR("From Type %d not supported yet...", node->type); }
  }

  return result;
}

// This function takes in a Postgres ColumnRef parsenode and transfer into
// a Peloton tuple value expression.
expression::AbstractExpression* PostgresParser::ColumnRefTransform(
    ColumnRef* root) {
  expression::AbstractExpression* result = NULL;
  List* fields = root->fields;
  switch (((Node*)(fields->head->data.ptr_value))->type) {
    case T_String: {
      if (fields->length == 1) {
        result = new expression::TupleValueExpression(
            std::string(((value*)(fields->head->data.ptr_value))->val.str));
      } else {
        result = new expression::TupleValueExpression(
            std::string(
                ((value*)(fields->head->next->data.ptr_value))->val.str),
            std::string(((value*)(fields->head->data.ptr_value))->val.str));
      }
      break;
    }
    case T_A_Star: {
      result = new expression::StarExpression();
      break;
    }
    default: {
      LOG_ERROR("Type %d of ColumnRef not handled yet...\n",
                ((Node*)(fields->head->data.ptr_value))->type);
    }
  }

  return result;
}

// This function takes in groupClause and havingClause of a Postgres SelectStmt
// transfers into a Peloton GroupByDescription object.
// TODO: having clause is not handled yet, depends on AExprTransform
parser::GroupByDescription* PostgresParser::GroupByTransform(List* group,
                                                             Node* having) {
  if (group == NULL) {
    return NULL;
  }

  parser::GroupByDescription* result = new parser::GroupByDescription();
  result->columns = new std::vector<expression::AbstractExpression*>();
  for (auto cell = group->head; cell != NULL; cell = cell->next) {
    Node* temp = (Node*)cell->data.ptr_value;
    switch (temp->type) {
      case T_ColumnRef: {
        result->columns->push_back(ColumnRefTransform((ColumnRef*)temp));
        break;
      }
      default: { LOG_ERROR("Group By type %d not supported...", temp->type); }
    }
  }

  // having clauses not implemented yet, depends on AExprTransform
  if (having != NULL) {
    LOG_ERROR("HAVING not implemented yet...\n");
  }
  return result;
}

// This function takes in the sortClause part of a Postgres SelectStmt
// parsenode and transfers it into a list of Peloton OrderDescription objects
// std::vector<parser::OrderDescription>* PostgresParser::OrderByTransform(List*
// order) {
parser::OrderDescription* PostgresParser::OrderByTransform(List* order) {
  if (order == NULL) {
    return NULL;
  }

  // std::vector<parser::OrderDescription>* result =
  //   new std::vector<parser::OrderDescription>();

  parser::OrderDescription* result = NULL;

  for (auto cell = order->head; cell != NULL; cell = cell->next) {
    Node* temp = (Node*)cell->data.ptr_value;
    if (temp->type == T_SortBy) {
      SortBy* sort = (SortBy*)temp;
      Node* target = sort->node;
      if (target->type == T_ColumnRef) {
        if (sort->sortby_dir == SORTBY_ASC) {
          // result->push_back(parser::OrderDescription(
          //                     parser::OrderType::kOrderAsc,
          //                     ColumnRefTransform((ColumnRef *)target)));
          result = new parser::OrderDescription(
              parser::OrderType::kOrderAsc,
              ColumnRefTransform((ColumnRef*)target));
        } else if (sort->sortby_dir == SORTBY_DESC) {
          // result->push_back(parser::OrderDescription(
          //                     parser::OrderType::kOrderDesc,
          //                     ColumnRefTransform((ColumnRef *)target)));
          result = new parser::OrderDescription(
              parser::OrderType::kOrderDesc,
              ColumnRefTransform((ColumnRef*)target));
        }

      } else {
        LOG_ERROR("SortBy type %d not supported...", target->type);
      }
    } else {
      LOG_ERROR("ORDER BY list member type %d\n", temp->type);
    }
    break;
  }
  return result;
}

// This function takes in a Posgres A_Const parsenode and transfers it into
// a Peloton constant value expression.
expression::AbstractExpression* PostgresParser::ConstTransform(A_Const* root) {
  expression::AbstractExpression* result = NULL;
  switch (root->val.type) {
    case T_Integer: {
      result = new expression::ConstantValueExpression(
          type::ValueFactory::GetIntegerValue((int64_t)root->val.val.ival));
      break;
    }
    case T_String: {
      result = new expression::ConstantValueExpression(
          type::ValueFactory::GetVarcharValue(std::string(root->val.val.str)));
      break;
    }
    default: {
      LOG_ERROR("A_Const type %d not supported yet...\n", root->type);
    }
  }

  return result;
}

// This function takes in a Postgres FuncCall parsenode and transfers it into
// a Peloton FunctionExpression object.
// TODO: support function calls on a single column.
expression::AbstractExpression* PostgresParser::FuncCallTransform(
    FuncCall* root) {
  expression::AbstractExpression* result = NULL;

  if (root->agg_star) {
    std::vector<expression::AbstractExpression*> children;
    result = new expression::FunctionExpression(
        ((value*)(root->funcname->head->data.ptr_value))->val.str, children);
  } else {
    LOG_ERROR("Aggregation over certain column not supported yet...\n");
  }
  return result;
}

// This function takes in the whereClause part of a Postgres SelectStmt
// parsenode and transfers it into the select_list of a Peloton SelectStatement.
// It checks the type of each target and call the corresponding helpers.
std::vector<expression::AbstractExpression*>* PostgresParser::TargetTransform(
    List* root) {
  std::vector<expression::AbstractExpression*>* result =
      new std::vector<expression::AbstractExpression*>();
  for (auto cell = root->head; cell != NULL; cell = cell->next) {
    ResTarget* target = (ResTarget*)(cell->data.ptr_value);
    switch (target->val->type) {
      case T_ColumnRef: {
        result->push_back(ColumnRefTransform((ColumnRef*)(target->val)));
        break;
      }
      case T_A_Const: {
        result->push_back(ConstTransform((A_Const*)(target->val)));
        break;
      }
      case T_FuncCall: {
        result->push_back(FuncCallTransform((FuncCall*)(target->val)));
        break;
      }
      default: {
        LOG_ERROR("Target type %d not suported yet...\n", target->val->type);
      }
    }
  }
  return result;
}

// This function takes in a Postgres BoolExpr parsenode and transfers into
// a Peloton conjunction expression.
expression::AbstractExpression* PostgresParser::BoolExprTransform(
    BoolExpr* root) {
  expression::AbstractExpression* result = NULL;
  expression::AbstractExpression* left = NULL;
  expression::AbstractExpression* right = NULL;
  LOG_TRACE("BoolExpr arg length %d\n", root->args->length);
  Node* node = (Node*)(root->args->head->next->data.ptr_value);
  switch (node->type) {
    case T_BoolExpr: {
      right = BoolExprTransform((BoolExpr*)node);
      break;
    }
    case T_A_Expr: {
      right = AExprTransform((A_Expr*)node);
      break;
    }
    default: {
      LOG_ERROR("BoolExpr arg type %d not suported yet...\n", node->type);
      return NULL;
    }
  }
  switch (root->boolop) {
    case AND_EXPR: {
      result = new expression::ConjunctionExpression(
          StringToExpressionType("CONJUNCTION_AND"), left, right);
      break;
    }
    case OR_EXPR: {
      result = new expression::ConjunctionExpression(
          StringToExpressionType("CONJUNCTION_OR"), left, right);
      break;
    }
    default: {
      LOG_ERROR("NOT_EXPR not supported yet...\n");
      return NULL;
    }
  }
  return result;
}

// This function takes in a Postgres A_Expr parsenode and transfers
// it into Peloton AbstractExpression.
// TODO: the whole function, needs a function that transforms strings
// of operators to Peloton expression type (e.g. ">" to COMPARE_GREATERTHAN)
expression::AbstractExpression* PostgresParser::AExprTransform(A_Expr* root) {
  if (root == NULL) {
    return NULL;
  }
  LOG_TRACE("A_Expr type: %d\n", root->type);
  return NULL;
}

// This function takes in the whereClause part of a Postgres SelectStmt
// parsenode and transfers it into Peloton AbstractExpression.
expression::AbstractExpression* PostgresParser::WhereTransform(Node* root) {
  if (root == NULL) {
    return NULL;
  }
  expression::AbstractExpression* result = NULL;
  switch (root->type) {
    case T_A_Expr: {
      result = AExprTransform((A_Expr*)root);
      break;
    }
    case T_BoolExpr: {
      result = BoolExprTransform((BoolExpr*)root);
      break;
    }
    default: { LOG_ERROR("WHERE of type %d not supported yet...", root->type); }
  }
  return result;
}

// This function takes in a Postgres SelectStmt parsenode
// and transfers into a Peloton SelectStatement parsenode.
// Please refer to parser/parsenode.h for the definition of
// SelectStmt parsenodes.
parser::SQLStatement* PostgresParser::SelectTransform(SelectStmt* root) {
  parser::SelectStatement* result = new parser::SelectStatement();
  result->select_list = TargetTransform(root->targetList);
  result->from_table = FromTransform(root->fromClause);
  result->group_by = GroupByTransform(root->groupClause, root->havingClause);
  result->order = OrderByTransform(root->sortClause);
  result->where_clause = WhereTransform(root->whereClause);
  return (parser::SQLStatement*)result;
}

// This function transfers a single Postgres statement into
// a Peloton SQLStatement object. It checks the type of
// Postgres parsenode of the input and call the corresponding
// helper function.
parser::SQLStatement* PostgresParser::NodeTransform(ListCell* stmt) {
  parser::SQLStatement* result = NULL;
  switch (((List*)(stmt->data.ptr_value))->type) {
    case T_SelectStmt: {
      result = SelectTransform((SelectStmt*)stmt->data.ptr_value);
      break;
    }
    default:
      break;
  }
  return result;
}

// This function transfers a list of Postgres statements into
// a Peloton SQLStatementList object. It traverses the parse list
// and call the helper for singles nodes.
parser::SQLStatementList* PostgresParser::ListTransform(List* root) {
  auto result = new parser::SQLStatementList();
  if (root == NULL) {
    return NULL;
  }
  LOG_TRACE("%d statements in total\n", (root->length));
  for (auto cell = root->head; cell != NULL; cell = cell->next) {
    result->AddStatement(NodeTransform(cell));
  }
  return result;
}

PgQueryInternalParsetreeAndError PostgresParser::ParseSQLString(
    const char* text) {
  return pg_query_parse(text);
}

PgQueryInternalParsetreeAndError PostgresParser::ParseSQLString(
    const std::string& text) {
  return ParseSQLString(text.c_str());
}

PostgresParser& PostgresParser::GetInstance() {
  static PostgresParser parser;
  return parser;
}

std::unique_ptr<parser::SQLStatementList> PostgresParser::BuildParseTree(
    const std::string& query_string) {
  auto stmt = PostgresParser::ParseSQLString(query_string);

  // when an error happends, the stme.stderr_buffer will be NULL and
  // corresponding error is in stmt.error
  if (stmt.stderr_buffer == NULL) {
    LOG_ERROR("%s at %d\n", stmt.error->message, stmt.error->cursorpos);
    auto new_stmt = std::unique_ptr<parser::SQLStatementList>(
        new parser::SQLStatementList());
    new_stmt->is_valid = false;
    LOG_INFO("new_stmt valid is %d\n", new_stmt->is_valid);
    return std::move(new_stmt);
  }

  delete stmt.stderr_buffer;
  auto transform_result = ListTransform(stmt.tree);

  auto new_stmt = std::unique_ptr<parser::SQLStatementList>(transform_result);
  LOG_INFO("new_stmt valid is %d\n", new_stmt->is_valid);
  return std::move(new_stmt);
}

}  // End pgparser namespace
}  // End peloton namespace
