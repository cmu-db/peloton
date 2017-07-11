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
#include <unordered_set>
#include <include/parser/pg_list.h>
#include <include/parser/pg_query.h>

#include "common/exception.h"
#include "expression/aggregate_expression.h"
#include "expression/case_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/function_expression.h"
#include "expression/operator_expression.h"
#include "expression/star_expression.h"
#include "expression/tuple_value_expression.h"
#include "parser/pg_query.h"
#include "parser/pg_list.h"
#include "parser/postgresparser.h"
#include "type/types.h"
#include "type/value_factory.h"

namespace peloton {
namespace parser {

PostgresParser::PostgresParser() {}

PostgresParser::~PostgresParser() {}

// This function takes a Postgres Alias parsenode and extracts the name of
// the alias and return a duplicate of the string.
char* PostgresParser::AliasTransform(Alias* root) {
  if (root == nullptr) {
    return nullptr;
  }

  return cstrdup(root->aliasname);
}

// This function takes a Postgres JoinExpr parsenode and transfers it to a
// Peloton JoinDefinition object. Depends on AExprTransform and
// BoolExprTransform.
parser::JoinDefinition* PostgresParser::JoinTransform(JoinExpr* root) {
  parser::JoinDefinition* result = nullptr;

  // Natrual join is not supported
  if ((root->jointype > 4) || (root->isNatural)) {
    return nullptr;
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
      delete result;
      throw NotImplementedException(StringUtil::Format(
          "Join type %d not supported yet...\n", root->jointype));
    }
  }

  // Check the type of left arg and right arg before transform
  if (root->larg->type == T_RangeVar) {
    result->left = RangeVarTransform(reinterpret_cast<RangeVar*>(root->larg));
  } else if (root->larg->type == T_RangeSubselect) {
    result->left =
        RangeSubselectTransform(reinterpret_cast<RangeSubselect*>(root->larg));
  } else if (root->larg->type == T_JoinExpr) {
    TableRef *l_table_ref = new TableRef(TableReferenceType::JOIN);
    l_table_ref->join =
        JoinTransform(reinterpret_cast<JoinExpr*>(root->larg));
    result->left = l_table_ref;
  } else {
    delete result;
    throw NotImplementedException(StringUtil::Format(
        "Join arg type %d not supported yet...\n", root->larg->type));
  }
  if (root->rarg->type == T_RangeVar) {
    result->right = RangeVarTransform(reinterpret_cast<RangeVar*>(root->rarg));
  } else if (root->rarg->type == T_RangeSubselect) {
    result->right = 
        RangeSubselectTransform(reinterpret_cast<RangeSubselect*>(root->rarg));
  } else if (root->rarg->type == T_JoinExpr) {

    TableRef *r_table_ref = new TableRef(TableReferenceType::JOIN);
    r_table_ref->join =
        JoinTransform(reinterpret_cast<JoinExpr*>(root->rarg));
    result->right = r_table_ref;
  } else {
    delete result;
    throw NotImplementedException(StringUtil::Format(
        "Join arg type %d not supported yet...\n", root->larg->type));
    return nullptr;
  }

  // transform the quals, depends on AExprTranform and BoolExprTransform
  switch (root->quals->type) {
    case T_A_Expr: {
      result->condition =
          AExprTransform(reinterpret_cast<A_Expr*>(root->quals));
      break;
    }
    case T_BoolExpr: {
      result->condition =
          BoolExprTransform(reinterpret_cast<BoolExpr*>(root->quals));
      break;
    }
    default: {
      delete result;
      throw NotImplementedException(StringUtil::Format(
          "Join quals type %d not supported yet...\n", root->larg->type));
    }
  }
  return result;
}

// This function takes in a single Postgres RangeVar parsenode and transfer
// it into a Peloton TableRef object.
parser::TableRef* PostgresParser::RangeVarTransform(RangeVar* root) {
  parser::TableRef* result =
      new parser::TableRef(StringToTableReferenceType("name"));
  result->table_info_ = new parser::TableInfo();

  if (root->schemaname) {
    result->schema = cstrdup(root->schemaname);
    result->table_info_->database_name = cstrdup(root->schemaname);
  }

  // parse alias
  result->alias = AliasTransform(root->alias);

  if (root->relname) {
    result->table_info_->table_name = cstrdup(root->relname);
  }

  if (root->catalogname) {
    result->table_info_->database_name = cstrdup(root->catalogname);
  }
  return result;
}

// This function takes in a single Postgres RangeVar parsenode and transfer
// it into a Peloton TableRef object.
parser::TableRef* PostgresParser::RangeSubselectTransform(
    RangeSubselect* root) {
  auto result = new parser::TableRef(StringToTableReferenceType("select"));
  result->select = reinterpret_cast<parser::SelectStatement*>(
      SelectTransform(reinterpret_cast<SelectStmt*>(root->subquery)));
  result->alias = AliasTransform(root->alias);
  if (result->select == nullptr) {
    delete result;
    result = nullptr;
  }

  return result;
}

// Get in a target list and check if is with variables
bool IsTargetListWithVariable(List* target_list) {
  // The only valid situation of a null from list is that all targets are constant
  for (auto cell = target_list->head; cell != nullptr; cell = cell->next) {
    ResTarget* target = reinterpret_cast<ResTarget*>(cell->data.ptr_value);
    LOG_TRACE("Type: %d", target->type);

    // Bypass the target nodes with type:
    // constant("SELECT 1;"), expression ("SELECT 1 + 1"),
    // and boolean ("SELECT 1!=2;");
    // TODO: We may want to see if there are more types to check.
    switch (target->val->type) {
      case T_A_Const:
      case T_A_Expr:
      case T_BoolExpr:
        continue;
      default:
        return true;
    }
  }
  return false;
}

// This fucntion takes in fromClause of a Postgres SelectStmt and transfers
// into a Peloton TableRef object.
// TODO: support select from multiple sources, nested queries, various joins
parser::TableRef* PostgresParser::FromTransform(SelectStmt* select_root) {
  // now support select from only one sources

  List* root = select_root->fromClause;
  /* Statement like 'SELECT *;' cannot detect by postgres parser and would lead to
   * a null list of from clause*/
  if (root == nullptr) {
    auto target_list = select_root->targetList;
    // The only valid situation of a null 'from list' is that all targets are constant
    LOG_TRACE("size is : %d", target_list->length);
    //print_pg_parse_tree(target_list);
    if (IsTargetListWithVariable(target_list)) {
      throw ParserException("Error parsing SQL statement");
    }
    return nullptr;
  }

  parser::TableRef* result = nullptr;
  Node* node;
  if (root->length > 1) {
    result = new TableRef(StringToTableReferenceType("CROSS_PRODUCT"));
    result->list = new std::vector<TableRef*>();
    for (auto cell = root->head; cell != nullptr; cell = cell->next) {
      node = reinterpret_cast<Node*>(cell->data.ptr_value);
      switch (node->type) {
        case T_RangeVar: {
          result->list->push_back(
              RangeVarTransform(reinterpret_cast<RangeVar*>(node)));
          break;
        }
        case T_RangeSubselect: {
          result->list->push_back(
              RangeSubselectTransform(reinterpret_cast<RangeSubselect*>(node)));
          break;
        }
        default: {
          delete result;
          throw NotImplementedException(StringUtil::Format(
              "From Type %d not supported yet...", node->type));
        }
      }
    }
    return result;
  }

  node = reinterpret_cast<Node*>(root->head->data.ptr_value);
  switch (node->type) {
    case T_RangeVar: {
      result = RangeVarTransform(reinterpret_cast<RangeVar*>(node));
      break;
    }
    case T_JoinExpr: {
      result = new parser::TableRef(StringToTableReferenceType("join"));
      result->join = JoinTransform(reinterpret_cast<JoinExpr*>(node));
      if (result->join == nullptr) {
        delete result;
        result = nullptr;
      }
      break;
    }
    case T_RangeSubselect: {
      result = RangeSubselectTransform(reinterpret_cast<RangeSubselect*>(node));
      break;
    }
    default: {
      throw NotImplementedException(
          StringUtil::Format("From Type %d not supported yet...", node->type));
    }
  }

  return result;
}

// This function takes in a Postgres ColumnRef parsenode and transfer into
// a Peloton tuple value expression.
expression::AbstractExpression* PostgresParser::ColumnRefTransform(
    ColumnRef* root) {
  expression::AbstractExpression* result = nullptr;
  List* fields = root->fields;
  switch ((reinterpret_cast<Node*>(fields->head->data.ptr_value))->type) {
    case T_String: {
      if (fields->length == 1) {
        result = new expression::TupleValueExpression(std::string(
            (reinterpret_cast<value*>(fields->head->data.ptr_value))->val.str));
      } else {
        result = new expression::TupleValueExpression(
            std::string((reinterpret_cast<value*>(
                             fields->head->next->data.ptr_value))->val.str),
            std::string((reinterpret_cast<value*>(fields->head->data.ptr_value))
                            ->val.str));
      }
      break;
    }
    case T_A_Star: {
      result = new expression::StarExpression();
      break;
    }
    default: {
      throw NotImplementedException(StringUtil::Format(
          "Type %d of ColumnRef not handled yet...\n",
          (reinterpret_cast<Node*>(fields->head->data.ptr_value))->type));
    }
  }

  return result;
}

// This function takes in a Postgres ParamRef parsenode and transfer into
// a Peloton tuple value expression.
expression::AbstractExpression* PostgresParser::ParamRefTransform(
    ParamRef* root) {
  //  LOG_INFO("Parameter number: %d", root->number);
  expression::AbstractExpression* res =
      new expression::ParameterValueExpression(root->number - 1);
  return res;
}

// This function takes in the Case Expression of a Postgres SelectStmt
// parsenode and transfers it into Peloton AbstractExpression.
expression::AbstractExpression* PostgresParser::CaseExprTransform(
    CaseExpr* root) {

  if (root == nullptr) {
    return nullptr;
  }

  // Transform the CASE argument
  auto arg_expr = ExprTransform(reinterpret_cast<Node*>(root->arg));

  // Transform the WHEN conditions
  std::vector<expression::CaseExpression::WhenClause> clauses;
  for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {

    CaseWhen *w = reinterpret_cast<CaseWhen*>(cell->data.ptr_value);

    // When condition
    auto when_expr = ExprTransform(reinterpret_cast<Node*>(w->expr));;

    // Result
    auto result_expr = ExprTransform(reinterpret_cast<Node*>(w->result));

    // Build When Clause and add it to the list
    clauses.push_back(expression::CaseExpression::WhenClause(
        expression::CaseExpression::AbsExprPtr(when_expr),
        expression::CaseExpression::AbsExprPtr(result_expr)));
  }

  // Transform the default result
  auto defresult_expr = ExprTransform(reinterpret_cast<Node*>(root->defresult));

  // Build Case Expression
  return arg_expr != nullptr ?
      new expression::CaseExpression(clauses.at(0).second.get()->GetValueType(),
          expression::CaseExpression::AbsExprPtr(arg_expr),
          clauses, expression::CaseExpression::AbsExprPtr(defresult_expr)) :
      new expression::CaseExpression(clauses.at(0).second.get()->GetValueType(),
          clauses, expression::CaseExpression::AbsExprPtr(defresult_expr));
}

// This function takes in groupClause and havingClause of a Postgres SelectStmt
// transfers into a Peloton GroupByDescription object.
parser::GroupByDescription* PostgresParser::GroupByTransform(List* group,
                                                             Node* having) {
  if (group == nullptr) {
    return nullptr;
  }

  parser::GroupByDescription* result = new parser::GroupByDescription();
  result->columns = new std::vector<expression::AbstractExpression*>();
  for (auto cell = group->head; cell != nullptr; cell = cell->next) {
    Node* temp = reinterpret_cast<Node*>(cell->data.ptr_value);
    try {
      result->columns->push_back(ExprTransform(temp));
    }
    catch(NotImplementedException e) {
      delete result;
      throw NotImplementedException(
          StringUtil::Format("Exception thrown in group by expr:\n%s", e.what()));
    }
  }

  // having clauses not implemented yet, depends on AExprTransform
  if (having != nullptr) {
    try {
      result->having = ExprTransform(having);
    }
    catch(NotImplementedException e) {
      delete result;
      throw NotImplementedException(
          StringUtil::Format("Exception thrown in having expr:\n%s", e.what()));
    }
  }
  return result;
}

// This function takes in the sortClause part of a Postgres SelectStmt
// parsenode and transfers it into a list of Peloton OrderDescription objects
// std::vector<parser::OrderDescription>* PostgresParser::OrderByTransform(List*
// order) {
parser::OrderDescription* PostgresParser::OrderByTransform(List* order) {
  if (order == nullptr) {
    return nullptr;
  }

  parser::OrderDescription* result = new OrderDescription();
  std::vector<OrderType>* types = new std::vector<OrderType>();
  std::vector<expression::AbstractExpression*>* exprs =
      new std::vector<expression::AbstractExpression*>();

  for (auto cell = order->head; cell != nullptr; cell = cell->next) {
    Node* temp = reinterpret_cast<Node*>(cell->data.ptr_value);
    if (temp->type == T_SortBy) {
      SortBy* sort = reinterpret_cast<SortBy*>(temp);
      Node* target = sort->node;
      if (sort->sortby_dir == SORTBY_ASC || sort->sortby_dir == SORTBY_DEFAULT)
        types->push_back(parser::kOrderAsc);
      if (sort->sortby_dir == SORTBY_DESC) types->push_back(parser::kOrderDesc);
      expression::AbstractExpression* expr = nullptr;
      try {
        expr = ExprTransform(target);
      }
      catch(NotImplementedException e) {
        throw NotImplementedException(
            StringUtil::Format("Exception thrown in order by expr:\n%s", e.what()));
      }
      exprs->push_back(expr);
    } else {
      throw NotImplementedException(
          StringUtil::Format("ORDER BY list member type %d\n", temp->type));
    }
  }
  result->exprs = exprs;
  result->types = types;
  return result;
}
// This function takes in a Posgres value parsenode and transfers it into
// a Peloton constant value expression.
expression::AbstractExpression* PostgresParser::ValueTransform(value val) {
  expression::AbstractExpression* result = nullptr;
  switch (val.type) {
    case T_Integer:
      result = new expression::ConstantValueExpression(
          type::ValueFactory::GetIntegerValue((int32_t)val.val.ival));
      break;
    case T_String:
      result = new expression::ConstantValueExpression(
          type::ValueFactory::GetVarcharValue(std::string(val.val.str)));
      break;
    case T_Float:
      result = new expression::ConstantValueExpression(
          type::ValueFactory::GetDecimalValue(
              std::stod(std::string(val.val.str))));
      break;
    case T_Null:
      result = new expression::ConstantValueExpression(
          type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER));
      break;
    default:
      throw NotImplementedException(
          StringUtil::Format("Value type %d not supported yet...\n", val.type));
  }
  return result;
}

// This function takes in a Posgres A_Const parsenode and transfers it into
// a Peloton constant value expression.
expression::AbstractExpression* PostgresParser::ConstTransform(A_Const* root) {
  return ValueTransform(root->val);
}

expression::AbstractExpression* PostgresParser::FuncCallTransform(
    FuncCall* root) {
  expression::AbstractExpression* result = nullptr;
  std::string fun_name = StringUtil::Lower(
      (reinterpret_cast<value*>(root->funcname->head->data.ptr_value))
          ->val.str);

  if (!IsAggregateFunction(fun_name)) {
    // Normal functions (i.e. built-in functions or UDFs)
    fun_name = (reinterpret_cast<value*>(root->funcname->tail->data.ptr_value))
                   ->val.str;
    std::vector<expression::AbstractExpression*> children;
    for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
      auto expr_node = (Node*) cell->data.ptr_value;
      expression::AbstractExpression* child_expr = nullptr;
      try {
        child_expr = ExprTransform(expr_node);
      }
      catch(NotImplementedException e) {
        throw NotImplementedException(
            StringUtil::Format("Exception thrown in function expr:\n%s", e.what()));
      }
      children.push_back(child_expr);
    }
    result = new expression::FunctionExpression(fun_name.c_str(), children);
  } else {
    // Aggregate function
    auto agg_fun_type = StringToExpressionType("AGGREGATE_" + fun_name);
    if (root->agg_star) {
      expression::AbstractExpression* children =
          new expression::StarExpression();
      result =
          new expression::AggregateExpression(agg_fun_type, false, children);
    } else {
      if (root->args->length < 2) {
        // auto children_expr_list = TargetTransform(root->args);
        expression::AbstractExpression *child;
        auto expr_node = (Node *) root->args->head->data.ptr_value;
        try {
          child = ExprTransform(expr_node);
        }
        catch(NotImplementedException e) {
          throw NotImplementedException(
              StringUtil::Format("Exception thrown in aggregation function:\n%s", e.what()));
        }
        result = new expression::AggregateExpression(agg_fun_type,
                                                     root->agg_distinct, child);
      } else {
        throw NotImplementedException(
            "Aggregation over multiple columns not supported yet...\n");
      }
    }
  }
  return result;
}

// This function takes in the whereClause part of a Postgres SelectStmt
// parsenode and transfers it into the select_list of a Peloton SelectStatement.
// It checks the type of each target and call the corresponding helpers.
std::vector<expression::AbstractExpression*>* PostgresParser::TargetTransform(
    List* root) {
  // Statement like 'SELECT;' cannot detect by postgres parser and would lead to
  // null list
  if (root == nullptr) {
      throw ParserException("Error parsing SQL statement");
  }

  std::vector<expression::AbstractExpression*>* result =
      new std::vector<expression::AbstractExpression*>();
  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    ResTarget* target = reinterpret_cast<ResTarget*>(cell->data.ptr_value);
    expression::AbstractExpression* expr = nullptr;
    try {
      expr = ExprTransform(target->val);
    }
    catch(NotImplementedException e) {
      throw NotImplementedException(
          StringUtil::Format("Exception thrown in target val:\n%s", e.what()));
    }
    if (target->name != nullptr) expr->alias = target->name;
    result->push_back(expr);
  }
  return result;
}

// This function takes in a Postgres BoolExpr parsenode and transfers into
// a Peloton conjunction expression.
expression::AbstractExpression* PostgresParser::BoolExprTransform(
    BoolExpr* root) {
  expression::AbstractExpression* result = nullptr;
  expression::AbstractExpression* next = nullptr;
  for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
    Node* node = reinterpret_cast<Node*>(cell->data.ptr_value);
    try {
      next = ExprTransform(node);
    }
    catch(NotImplementedException e) {
      throw NotImplementedException(
          StringUtil::Format("Exception thrown in boolean expr:\n%s", e.what()));
    }
    switch (root->boolop) {
      case AND_EXPR: {
        if (result == nullptr)
          result = next;
        else
          result = new expression::ConjunctionExpression(
              StringToExpressionType("CONJUNCTION_AND"), result, next);
        break;
      }
      case OR_EXPR: {
        if (result == nullptr)
          result = next;
        else
          result = new expression::ConjunctionExpression(
              StringToExpressionType("CONJUNCTION_OR"), result, next);
        break;
      }
      case NOT_EXPR: {
        result = new expression::OperatorExpression(
            StringToExpressionType("OPERATOR_NOT"), StringToTypeId("INVALID"),
            next, nullptr);
        break;
      }
    }
  }
  return result;
}

expression::AbstractExpression* PostgresParser::ExprTransform(Node* node) {

  if (node == nullptr) {
    return nullptr;
  }

  expression::AbstractExpression* expr = nullptr;
  switch (node->type) {
    case T_ColumnRef: {
      expr = ColumnRefTransform(reinterpret_cast<ColumnRef*>(node));
      break;
    }
    case T_A_Const: {
      expr = ConstTransform(reinterpret_cast<A_Const*>(node));
      break;
    }
    case T_A_Expr: {
      expr = AExprTransform(reinterpret_cast<A_Expr*>(node));
      break;
    }
    case T_ParamRef: {
      expr = ParamRefTransform(reinterpret_cast<ParamRef*>(node));
      break;
    }
    case T_FuncCall: {
      expr = FuncCallTransform(reinterpret_cast<FuncCall*>(node));
      break;
    }
    case T_BoolExpr: {
      expr = BoolExprTransform(reinterpret_cast<BoolExpr*>(node));
      break;
    }
    case T_CaseExpr: {
      expr = CaseExprTransform(reinterpret_cast<CaseExpr*>(node));
      break;
    }
    default: {
      throw NotImplementedException(StringUtil::Format(
          "Expr of type %d not supported yet...\n", node->type));
    }
  }
  return expr;
}

// This function takes in a Postgres A_Expr parsenode and transfers
// it into Peloton AbstractExpression.
// TODO: the whole function, needs a function that transforms strings
// of operators to Peloton expression type (e.g. ">" to COMPARE_GREATERTHAN)
expression::AbstractExpression* PostgresParser::AExprTransform(A_Expr* root) {
  if (root == nullptr) {
    return nullptr;
  }

  LOG_TRACE("A_Expr type: %d\n", root->type);

  UNUSED_ATTRIBUTE expression::AbstractExpression* result = nullptr;
  UNUSED_ATTRIBUTE ExpressionType target_type;
  const char* name =
      (reinterpret_cast<value*>(root->name->head->data.ptr_value))->val.str;
  if ((root->kind) != AEXPR_DISTINCT) {
    target_type = StringToExpressionType(std::string(name));
  } else {
    target_type = StringToExpressionType("COMPARE_DISTINCT_FROM");
  }
  if (target_type == ExpressionType::INVALID) {
    throw NotImplementedException(
        StringUtil::Format("COMPARE type %s not supported yet...\n", name));
  }

  expression::AbstractExpression* left_expr = nullptr;
  expression::AbstractExpression* right_expr = nullptr;

  try {
    left_expr = ExprTransform(root->lexpr);
  }
  catch(NotImplementedException e) {
    throw NotImplementedException(
        StringUtil::Format("Exception thrown in left expr:\n%s", e.what()));
  }
  try {
    right_expr = ExprTransform(root->rexpr);
  }
  catch(NotImplementedException e) {
    delete left_expr;
    throw NotImplementedException(
        StringUtil::Format("Exception thrown in right expr:\n%s", e.what()));
  }


  int type_id = static_cast<int>(target_type);
  if (type_id <= 6) {
    result = new expression::OperatorExpression(
        target_type, StringToTypeId("INVALID"), left_expr, right_expr);
  } else if (((10 <= type_id) && (type_id <= 17)) || (type_id == 20)) {
    result = new expression::ComparisonExpression(target_type, left_expr,
                                                  right_expr);
  } else {
    delete left_expr;
    delete right_expr;
    throw NotImplementedException(StringUtil::Format(
        "A_Expr Transform for type %d is not implemented yet...\n", type_id));
  }
  return result;
}

// This function takes in the whereClause part of a Postgres SelectStmt
// parsenode and transfers it into Peloton AbstractExpression.
expression::AbstractExpression* PostgresParser::WhereTransform(Node* root) {
  if (root == nullptr) {
    return nullptr;
  }
  expression::AbstractExpression* result = nullptr;

  try {
    result = ExprTransform(root);
  }
  catch(NotImplementedException e) {
    throw NotImplementedException(
        StringUtil::Format("Exception thrown in WHERE:\n%s", e.what()));
  }
  return result;
}

// This helper function takes in a Postgres ColumnDef object and transforms
// it into a Peloton ColumnDefinition object
parser::ColumnDefinition* PostgresParser::ColumnDefTransform(ColumnDef* root) {
  parser::ColumnDefinition::DataType data_type;
  TypeName* type_name = root->typeName;
  char* name = (reinterpret_cast<value*>(type_name->names->tail->data.ptr_value)
                    ->val.str);
  parser::ColumnDefinition* result = nullptr;

  // Transform column type
  if ((strcmp(name, "int") == 0) || (strcmp(name, "int4") == 0)) {
    data_type = ColumnDefinition::DataType::INT;
  } else if (strcmp(name, "varchar") == 0) {
    data_type = ColumnDefinition::DataType::VARCHAR;
  } else if (strcmp(name, "int8") == 0) {
    data_type = ColumnDefinition::DataType::BIGINT;
  } else if (strcmp(name, "int2") == 0) {
    data_type = ColumnDefinition::DataType::SMALLINT;
  } else if (strcmp(name, "timestamp") == 0) {
    data_type = ColumnDefinition::DataType::TIMESTAMP;
  } else if (strcmp(name, "bool") == 0) {
    data_type = ColumnDefinition::DataType::BOOLEAN;
  } else if (strcmp(name, "bpchar") == 0) {
    data_type = ColumnDefinition::DataType::CHAR;
  } else if ((strcmp(name, "double") == 0) || (strcmp(name, "float8") == 0)) {
    data_type = ColumnDefinition::DataType::DOUBLE;
  } else if ((strcmp(name, "real") == 0) || (strcmp(name, "float4") == 0)) {
    data_type = ColumnDefinition::DataType::FLOAT;
  } else if (strcmp(name, "numeric") == 0) {
    data_type = ColumnDefinition::DataType::DECIMAL;
  } else if (strcmp(name, "text") == 0) {
    data_type = ColumnDefinition::DataType::TEXT;
  } else if (strcmp(name, "tinyint") == 0) {
    data_type = ColumnDefinition::DataType::TINYINT;
  } else if (strcmp(name, "varbinary") == 0) {
    data_type = ColumnDefinition::DataType::VARBINARY;
  } else {
    throw NotImplementedException(
        StringUtil::Format("Column DataType %s not supported yet...\n", name));
  }

  // Transform Varchar len
  result = new ColumnDefinition(cstrdup(root->colname), data_type);
  if (type_name->typmods) {
    Node* node =
        reinterpret_cast<Node*>(type_name->typmods->head->data.ptr_value);
    if (node->type == T_A_Const) {
      if (reinterpret_cast<A_Const*>(node)->val.type != T_Integer) {
        delete result;
        throw NotImplementedException(
            StringUtil::Format("typmods of type %d not supported yet...\n",
                               reinterpret_cast<A_Const*>(node)->val.type));
      }
      result->varlen =
          static_cast<size_t>(reinterpret_cast<A_Const*>(node)->val.val.ival);
    } else {
      delete result;
      throw NotImplementedException(StringUtil::Format(
          "typmods of type %d not supported yet...\n", node->type));
    }
  }

  // Transform Per-column constraints
  if (root->constraints) {
    for (auto cell = root->constraints->head; cell != nullptr;
         cell = cell->next) {
      auto constraint = reinterpret_cast<Constraint*>(cell->data.ptr_value);
      if (constraint->contype == CONSTR_PRIMARY)
        result->primary = true;
      else if (constraint->contype == CONSTR_NOTNULL)
        result->not_null = true;
      else if (constraint->contype == CONSTR_UNIQUE)
        result->unique = true;
      else if (constraint->contype == CONSTR_FOREIGN) {
        result->foreign_key_sink = new std::vector<char*>();
        result->table_info_ = new TableInfo();
        // Transform foreign key attributes
        // Reference table
        result->table_info_->table_name = cstrdup(constraint->pktable->relname);
        // Reference column
        if (constraint->pk_attrs != nullptr)
          for (auto attr_cell = constraint->pk_attrs->head;
               attr_cell != nullptr; attr_cell = attr_cell->next) {
            value* attr_val =
                reinterpret_cast<value*>(attr_cell->data.ptr_value);
            result->foreign_key_sink->push_back(cstrdup(attr_val->val.str));
          }
        // Action type
        result->foreign_key_delete_action =
            CharToActionType(constraint->fk_del_action);
        result->foreign_key_update_action =
            CharToActionType(constraint->fk_upd_action);
        // Match type
        result->foreign_key_match_type = CharToMatchType(constraint->fk_matchtype);
      }
      else if (constraint->contype == CONSTR_DEFAULT) {
        try {
          result->default_value = ExprTransform(constraint->raw_expr);
        }
        catch (NotImplementedException e) {
          throw NotImplementedException(
              StringUtil::Format("Exception thrown in default expr:\n%s", e.what()));
        }
      }
      else if (constraint->contype == CONSTR_CHECK) {
        try {
          result->check_expression = ExprTransform(constraint->raw_expr);
        }
        catch(NotImplementedException e) {
          throw NotImplementedException(
              StringUtil::Format("Exception thrown in check expr:\n%s", e.what()));
        }
      }
    }
  }

  return result;
}

// This function takes in a Postgres CreateStmt parsenode
// and transfers into a Peloton CreateStatement parsenode.
// Please refer to parser/parsenode.h for the definition of
// CreateStmt parsenodes.
parser::SQLStatement* PostgresParser::CreateTransform(CreateStmt* root) {
  UNUSED_ATTRIBUTE CreateStmt* temp = root;
  parser::CreateStatement* result =
      new CreateStatement(CreateStatement::CreateType::kTable);
  RangeVar* relation = root->relation;
  result->table_info_ = new parser::TableInfo();

  if (relation->relname) {
    result->table_info_->table_name = cstrdup(relation->relname);
  }
  if (relation->catalogname) {
    result->table_info_->database_name = cstrdup(relation->catalogname);
  };

  if (root->tableElts->length > 0) {
    result->columns = new std::vector<ColumnDefinition*>();
  }

  std::unordered_set<std::string> primary_keys;
  for (auto cell = root->tableElts->head; cell != nullptr; cell = cell->next) {
    Node* node = reinterpret_cast<Node*>(cell->data.ptr_value);
    if ((node->type) == T_ColumnDef) {
      // Transform Regular Column
      ColumnDefinition* temp =
          ColumnDefTransform(reinterpret_cast<ColumnDef*>(node));
      result->columns->push_back(temp);
    } else if (node->type == T_Constraint) {
      // Transform Constraints
      auto constraint = reinterpret_cast<Constraint*>(node);
      if (constraint->contype == CONSTR_PRIMARY) {
        for (auto key_cell = constraint->keys->head; key_cell != nullptr;
             key_cell = key_cell->next) {
          primary_keys.emplace(
              reinterpret_cast<value*>(key_cell->data.ptr_value)->val.str);
        }
      } else if (constraint->contype == CONSTR_FOREIGN) {
        auto col = new ColumnDefinition(ColumnDefinition::FOREIGN);
        col->table_info_ = new TableInfo();
        col->foreign_key_source = new std::vector<char*>();
        col->foreign_key_sink = new std::vector<char*>();
        // Transform foreign key attributes
        for (auto attr_cell = constraint->fk_attrs->head; attr_cell != nullptr;
             attr_cell = attr_cell->next) {
          value* attr_val = reinterpret_cast<value*>(attr_cell->data.ptr_value);
          col->foreign_key_source->push_back(cstrdup(attr_val->val.str));
        }
        // Transform Primary key attributes
        for (auto attr_cell = constraint->pk_attrs->head; attr_cell != nullptr;
             attr_cell = attr_cell->next) {
          value* attr_val = reinterpret_cast<value*>(attr_cell->data.ptr_value);
          col->foreign_key_sink->push_back(cstrdup(attr_val->val.str));
        }
        // Update Reference Table
        col->table_info_->table_name = cstrdup(constraint->pktable->relname);
        // Action type
        col->foreign_key_delete_action =
            CharToActionType(constraint->fk_del_action);
        col->foreign_key_update_action =
            CharToActionType(constraint->fk_upd_action);
        // Match type
        col->foreign_key_match_type = CharToMatchType(constraint->fk_matchtype);

        result->columns->push_back(col);
      } else {
        throw NotImplementedException(StringUtil::Format(
            "Constraint of type %d not supported yet", node->type));
      }
    } else {
      delete result->table_info_;
      delete result;
      throw NotImplementedException(StringUtil::Format(
          "tableElt of type %d not supported yet...", node->type));
    }
  }

  // Enforce Primary Keys
  for (auto column : *result->columns) {
    // Skip Foreign Key Constraint
    if (column->name == nullptr) continue;
    if (primary_keys.find(std::string(column->name)) != primary_keys.end()) {
      column->primary = true;
    }
  }

  return reinterpret_cast<parser::SQLStatement*>(result);
}

// This function takes in a Postgres IndexStmt parsenode
// and transfers into a Peloton CreateStatement parsenode.
// Please refer to parser/parsenode.h for the definition of
// IndexStmt parsenodes.
parser::SQLStatement* PostgresParser::CreateIndexTransform(IndexStmt* root) {
  parser::CreateStatement* result =
      new parser::CreateStatement(CreateStatement::kIndex);
  result->unique = root->unique;
  result->index_attrs = new std::vector<char*>;
  for (auto cell = root->indexParams->head; cell != nullptr;
       cell = cell->next) {
    char* index_attr = reinterpret_cast<IndexElem*>(cell->data.ptr_value)->name;
    result->index_attrs->push_back(cstrdup(index_attr));
  }
  result->index_type = IndexType::BWTREE;
  result->table_info_ = new TableInfo();
  result->table_info_->table_name = cstrdup(root->relation->relname);
  result->index_name = cstrdup(root->idxname);
  return result;
}

// This function takes in a Postgres CreatedbStmt parsenode
// and transfers into a Peloton CreateStatement parsenode.
// Please refer to parser/parsenode.h for the definition of
// CreatedbStmt parsenodes.
parser::SQLStatement* PostgresParser::CreateDbTransform(CreatedbStmt* root) {
  parser::CreateStatement* result =
      new parser::CreateStatement(CreateStatement::kDatabase);
  result->database_name = cstrdup(root->dbname);
  return result;
}

parser::DropStatement* PostgresParser::DropTransform(DropStmt* root) {
  auto res = new DropStatement(DropStatement::EntityType::kTable);
  for (auto cell = root->objects->head; cell != nullptr; cell = cell->next) {
    res->missing = root->missing_ok;
    auto table_info = new TableInfo{};
    auto table_list = reinterpret_cast<List*>(cell->data.ptr_value);
    LOG_INFO("%d", ((Node*)(table_list->head->data.ptr_value))->type);
    table_info->table_name = cstrdup(
        reinterpret_cast<value*>(table_list->head->data.ptr_value)->val.str);
    res->table_info_ = table_info;
    break;
  }
  return res;
}

parser::DeleteStatement* PostgresParser::TruncateTransform(TruncateStmt* root) {
  auto result = new DeleteStatement();
  for (auto cell = root->relations->head; cell != nullptr; cell = cell->next) {
    result->table_ref =
        RangeVarTransform(reinterpret_cast<RangeVar*>(cell->data.ptr_value));
    break;
  }
  return result;
}

parser::ExecuteStatement* PostgresParser::ExecuteTransform(ExecuteStmt* root) {
  auto result = new ExecuteStatement();
  result->name = cstrdup(root->name);
  if (root->params != nullptr)
    result->parameters = ParamListTransform(root->params);
  return result;
}

parser::PrepareStatement* PostgresParser::PrepareTransform(PrepareStmt* root) {
  auto res = new PrepareStatement();
  res->name = cstrdup(root->name);
  auto stmt_list = new SQLStatementList();
  stmt_list->statements.push_back(NodeTransform(root->query));
  res->query = stmt_list;
  return res;
}

// TODO: Only support COPY TABLE TO FILE and DELIMITER option
parser::CopyStatement* PostgresParser::CopyTransform(CopyStmt* root) {
  auto res = new CopyStatement(peloton::CopyType::EXPORT_OTHER);
  res->cpy_table = RangeVarTransform(root->relation);
  res->file_path = cstrdup(root->filename);
  for (auto cell = root->options->head; cell != NULL; cell = cell->next) {
    auto def_elem = reinterpret_cast<DefElem*>(cell->data.ptr_value);
    if (strcmp(def_elem->defname, "delimiter") == 0) {
      auto delimiter = reinterpret_cast<value*>(def_elem->arg)->val.str;
      res->delimiter = *delimiter;
      break;
    }
  }
  return res;
}

// Analyze statment is parsed with vacuum statment.
parser::AnalyzeStatement* PostgresParser::VacuumTransform(VacuumStmt* root) {
  if (root->options != VACOPT_ANALYZE) {
    LOG_TRACE("Vacuum not supported.");
    return nullptr;
  }
  auto res = new AnalyzeStatement();
  if (root->relation != NULL) { //TOOD: check NULL vs nullptr
    res->analyze_table = RangeVarTransform(root->relation);
  }
  res->analyze_columns = ColumnNameTransform(root->va_cols);
  return res;
}

std::vector<char*>* PostgresParser::ColumnNameTransform(List* root) {
  if (root == nullptr) return nullptr;

  std::vector<char*>* result = new std::vector<char*>();

  for (auto cell = root->head; cell != NULL; cell = cell->next) {
    ResTarget* target = (ResTarget*)(cell->data.ptr_value);
    result->push_back(cstrdup(target->name));
  }

  return result;
}

// This function takes in the valuesLists of a Postgres InsertStmt
// parsenode and transfers it into Peloton AbstractExpression.
// This is a vector pointer of vector pointers because one InsertStmt can insert
// multiple tuples.
std::vector<std::vector<expression::AbstractExpression*>*>*
PostgresParser::ValueListsTransform(List* root) {
  std::vector<std::vector<expression::AbstractExpression*>*>* result =
      new std::vector<std::vector<expression::AbstractExpression*>*>();

  for (auto value_list = root->head; value_list != NULL;
       value_list = value_list->next) {
    std::vector<expression::AbstractExpression*>* cur_result =
        new std::vector<expression::AbstractExpression*>();

    List* target = (List*)(value_list->data.ptr_value);
    for (auto cell = target->head; cell != NULL; cell = cell->next) {
      auto expr = reinterpret_cast<Expr*>(cell->data.ptr_value);
      if (expr->type == T_ParamRef)
        cur_result->push_back(ParamRefTransform((ParamRef*)expr));
      else if (expr->type == T_A_Const)
        cur_result->push_back(ConstTransform((A_Const*)expr));
      else if (expr->type == T_SetToDefault) {
        // TODO handle default type
        // add corresponding expression for
        // default to cur_result
        cur_result->push_back(nullptr);
      }
    }
    result->push_back(cur_result);
  }

  return result;
}

// This function takes in the paramlist of a Postgres ExecuteStmt
// parsenode and transfers it into Peloton AbstractExpression.
std::vector<expression::AbstractExpression*>*
PostgresParser::ParamListTransform(List* root) {
  std::vector<expression::AbstractExpression*>* result =
      new std::vector<expression::AbstractExpression*>();

  for (auto cell = root->head; cell != NULL; cell = cell->next) {
    result->push_back(ConstTransform((A_Const*)(cell->data.ptr_value)));
  }

  return result;
}

// This function takes in a Postgres InsertStmt parsenode
// and transfers into a Peloton InsertStatement.
// Please refer to parser/parsenode.h for the definition of
// SelectStmt parsenodes.
parser::SQLStatement* PostgresParser::InsertTransform(InsertStmt* root) {
  // selectStmt must exist. It would either select from table or directly select
  // some values
  PL_ASSERT(root->selectStmt != NULL);

  auto select_stmt = reinterpret_cast<SelectStmt*>(root->selectStmt);

  parser::InsertStatement* result = nullptr;
  if (select_stmt->fromClause != NULL) {
    // if select from a table to insert
    result = new parser::InsertStatement(InsertType::SELECT);

    result->select = reinterpret_cast<parser::SelectStatement*>(
        SelectTransform(select_stmt));

  } else {
    // Directly insert some values
    result = new parser::InsertStatement(InsertType::VALUES);

    PL_ASSERT(select_stmt->valuesLists != NULL);
    result->insert_values = ValueListsTransform(select_stmt->valuesLists);
  }

  // The table to insert into
  result->table_ref_ = RangeVarTransform((RangeVar*)(root->relation));

  // The columns to insert into
  result->columns = ColumnNameTransform(root->cols);
  return (parser::SQLStatement*)result;
}

// This function takes in a Postgres SelectStmt parsenode
// and transfers into a Peloton SelectStatement parsenode.
// Please refer to parser/parsenode.h for the definition of
// SelectStmt parsenodes.
parser::SQLStatement* PostgresParser::SelectTransform(SelectStmt* root) {
  parser::SelectStatement* result;
  switch (root->op) {
    case SETOP_NONE:
      result = new parser::SelectStatement();
      try {
        result->select_list = TargetTransform(root->targetList);
        result->from_table = FromTransform(root);
      } catch (ParserException &e) {
        delete (result);
        throw e;
      }
      result->select_distinct = root->distinctClause != NULL ? true : false;
      result->group_by =
          GroupByTransform(root->groupClause, root->havingClause);
      result->order = OrderByTransform(root->sortClause);
      result->where_clause = WhereTransform(root->whereClause);
      if (root->limitCount != nullptr) {
        int64_t limit =
            reinterpret_cast<A_Const*>(root->limitCount)->val.val.ival;
        int64_t offset = 0;
        if (root->limitOffset != nullptr)
          offset = reinterpret_cast<A_Const*>(root->limitOffset)->val.val.ival;
        result->limit = new LimitDescription(limit, offset);
      }
      break;
    case SETOP_UNION:
      result = reinterpret_cast<parser::SelectStatement*>(
          SelectTransform(root->larg));
      result->union_select = reinterpret_cast<parser::SelectStatement*>(
          SelectTransform(root->rarg));
      break;
    default:
      throw NotImplementedException(StringUtil::Format(
          "Set operation %d not supported yet...\n", root->op));
  }

  return reinterpret_cast<parser::SQLStatement*>(result);
}

// This function takes in a Postgres DeleteStmt parsenode
// and transfers into a Peloton DeleteStatement parsenode.
// Please refer to parser/parsenode.h for the definition of
// DeleteStmt parsenodes.
parser::SQLStatement* PostgresParser::DeleteTransform(DeleteStmt* root) {
  parser::DeleteStatement* result = new parser::DeleteStatement();
  result->table_ref = RangeVarTransform(root->relation);
  result->expr = WhereTransform(root->whereClause);
  return (parser::SQLStatement*)result;
}

// Transform Postgres TransacStmt into Peloton TransactionStmt
parser::TransactionStatement* PostgresParser::TransactionTransform(
    TransactionStmt* root) {
  if (root->kind == TRANS_STMT_BEGIN) {
    return new parser::TransactionStatement(TransactionStatement::kBegin);
  } else if (root->kind == TRANS_STMT_COMMIT) {
    return new parser::TransactionStatement(TransactionStatement::kCommit);
  } else if (root->kind == TRANS_STMT_ROLLBACK) {
    return new parser::TransactionStatement(TransactionStatement::kRollback);
  } else {
    throw NotImplementedException(StringUtil::Format(
        "Commmand type %d not supported yet.\n", root->kind));
  }
}

// This function transfers a single Postgres statement into
// a Peloton SQLStatement object. It checks the type of
// Postgres parsenode of the input and call the corresponding
// helper function.
parser::SQLStatement* PostgresParser::NodeTransform(Node* stmt) {
  parser::SQLStatement* result = nullptr;
  switch (stmt->type) {
    case T_SelectStmt:
      result = SelectTransform(reinterpret_cast<SelectStmt*>(stmt));
      break;
    case T_CreateStmt:
      result = CreateTransform(reinterpret_cast<CreateStmt*>(stmt));
      break;
    case T_IndexStmt:
      result = CreateIndexTransform(reinterpret_cast<IndexStmt*>(stmt));
      break;
    case T_UpdateStmt:
      result = UpdateTransform((UpdateStmt*)stmt);
      break;
    case T_DeleteStmt:
      result = DeleteTransform((DeleteStmt*)stmt);
      break;
    case T_InsertStmt:
      result = InsertTransform((InsertStmt*)stmt);
      break;
    case T_DropStmt:
      result = DropTransform((DropStmt*)stmt);
      break;
    case T_TruncateStmt:
      result = TruncateTransform((TruncateStmt*)stmt);
      break;
    case T_TransactionStmt:
      result = TransactionTransform((TransactionStmt*)stmt);
      break;
    case T_ExecuteStmt:
      result = ExecuteTransform((ExecuteStmt*)stmt);
      break;
    case T_PrepareStmt:
      result = PrepareTransform((PrepareStmt*)stmt);
      break;
    case T_CopyStmt:
      result = CopyTransform((CopyStmt*)stmt);
      break;
    case T_CreatedbStmt:
      result = CreateDbTransform((CreatedbStmt*)stmt);
      break;
    case T_VacuumStmt:
      result = VacuumTransform((VacuumStmt*)stmt);
      break;
    default: {
      throw NotImplementedException(StringUtil::Format(
          "Statement of type %d not supported yet...\n", stmt->type));
    }
  }
  return result;
}

// This function transfers a list of Postgres statements into
// a Peloton SQLStatementList object. It traverses the parse list
// and call the helper for singles nodes.
parser::SQLStatementList* PostgresParser::ListTransform(List* root) {
  auto result = new parser::SQLStatementList();
  if (root == nullptr) {
    return nullptr;
  }
  LOG_TRACE("%d statements in total\n", (root->length));
  try {
    for (auto cell = root->head; cell != nullptr; cell = cell->next) {
      result->AddStatement(NodeTransform((Node*)cell->data.ptr_value));
    }
  } catch (ParserException &e) {
    delete result;
    throw e;
  }

  return result;
}

std::vector<parser::UpdateClause*>* PostgresParser::UpdateTargetTransform(
    List* root) {
  auto result = new std::vector<parser::UpdateClause*>();
  for (auto cell = root->head; cell != NULL; cell = cell->next) {
    auto update_clause = new UpdateClause();
    ResTarget* target = (ResTarget*)(cell->data.ptr_value);
    update_clause->column = cstrdup(target->name);
    try {
      update_clause->value = ExprTransform(target->val);
    }
    catch(NotImplementedException e) {
      throw NotImplementedException(
          StringUtil::Format("Exception thrown in update expr:\n%s", e.what()));
    }
    result->push_back(update_clause);
  }
  return result;
}

// TODO: Not support with clause, from clause and returning list in update
// statement in peloton
parser::UpdateStatement* PostgresParser::UpdateTransform(
    UpdateStmt* update_stmt) {
  auto result = new parser::UpdateStatement();
  result->table = RangeVarTransform(update_stmt->relation);
  result->where = WhereTransform(update_stmt->whereClause);
  result->updates = UpdateTargetTransform(update_stmt->targetList);
  return result;
}

// Call postgres's parser and start transforming it into Peloton's parse tree
parser::SQLStatementList* PostgresParser::ParseSQLString(const char* text) {
  auto ctx = pg_query_parse_init();
  auto result = pg_query_parse(text);
  if (result.error) {
    // Parse Error
    LOG_ERROR("%s at %d\n", result.error->message, result.error->cursorpos);
    auto new_stmt = new parser::SQLStatementList();
    new_stmt->is_valid = false;
    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    return new_stmt;
  }

  // DEBUG only. Comment this out in release mode
  // print_pg_parse_tree(result.tree);
  parser::SQLStatementList* transform_result;
  try {
    transform_result = ListTransform(result.tree);
  } catch (Exception &e) {
    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    throw e;
  }

  pg_query_parse_finish(ctx);
  pg_query_free_parse_result(result);
  return transform_result;
}

parser::SQLStatementList* PostgresParser::ParseSQLString(
    const std::string& sql) {
  return ParseSQLString(sql.c_str());
}

PostgresParser& PostgresParser::GetInstance() {
  static PostgresParser parser;
  return parser;
}

std::unique_ptr<parser::SQLStatementList> PostgresParser::BuildParseTree(
    const std::string& query_string) {
  auto stmt = PostgresParser::ParseSQLString(query_string);

  LOG_TRACE("Number of statements: %lu", stmt->GetStatements().size());

  std::unique_ptr<parser::SQLStatementList> sql_stmt(stmt);
  return sql_stmt;
}

}  // End pgparser namespace
}  // End peloton namespace
