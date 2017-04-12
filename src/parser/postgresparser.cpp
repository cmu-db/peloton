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

#include "common/exception.h"
#include "expression/aggregate_expression.h"
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
      LOG_ERROR("Join type %d not supported yet...\n", root->jointype);
      throw NotImplementedException("...");
    }
  }

  // Check the type of left arg and right arg before transform
  if (root->larg->type == T_RangeVar) {
    result->left = RangeVarTransform(reinterpret_cast<RangeVar*>(root->larg));
  } else if (root->larg->type == T_RangeSubselect) {
    result->left =
        RangeSubselectTransform(reinterpret_cast<RangeSubselect*>(root->larg));
  } else {
    LOG_ERROR("Join arg type %d not supported yet...\n", root->larg->type);
    delete result;
    throw NotImplementedException("...");
  }
  if (root->rarg->type == T_RangeVar) {
    result->right = RangeVarTransform(reinterpret_cast<RangeVar*>(root->rarg));
  } else if (root->rarg->type == T_RangeSubselect) {
    result->right =
        RangeSubselectTransform(reinterpret_cast<RangeSubselect*>(root->rarg));
  } else {
    LOG_ERROR("Join arg type %d not supported yet...\n", root->larg->type);
    delete result;
    throw NotImplementedException("...");
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
      LOG_ERROR("Join quals type %d not supported yet...\n", root->larg->type);
      delete result;
      throw NotImplementedException("...");
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

// This fucntion takes in fromClause of a Postgres SelectStmt and transfers
// into a Peloton TableRef object.
// TODO: support select from multiple sources, nested queries, various joins
parser::TableRef* PostgresParser::FromTransform(List* root) {
  // now support select from only one sources
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
        default: { LOG_ERROR("From Type %d not supported yet...", node->type); }
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
    default: { LOG_ERROR("From Type %d not supported yet...", node->type); }
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
      LOG_ERROR("Type %d of ColumnRef not handled yet...\n",
                (reinterpret_cast<Node*>(fields->head->data.ptr_value))->type);
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

// This function takes in groupClause and havingClause of a Postgres SelectStmt
// transfers into a Peloton GroupByDescription object.
// TODO: having clause is not handled yet, depends on AExprTransform
parser::GroupByDescription* PostgresParser::GroupByTransform(List* group,
                                                             Node* having) {
  if (group == nullptr) {
    return nullptr;
  }

  parser::GroupByDescription* result = new parser::GroupByDescription();
  result->columns = new std::vector<expression::AbstractExpression*>();
  for (auto cell = group->head; cell != nullptr; cell = cell->next) {
    Node* temp = reinterpret_cast<Node*>(cell->data.ptr_value);
    switch (temp->type) {
      case T_ColumnRef:
        result->columns->push_back(ColumnRefTransform((ColumnRef*)temp));
        break;
      case T_A_Expr:
        result->columns->push_back(AExprTransform((A_Expr*)temp));
        break;
      default: {
        LOG_ERROR("Group By type %d not supported...", temp->type);
        throw NotImplementedException("");
      }
    }
  }

  // having clauses not implemented yet, depends on AExprTransform
  if (having != nullptr) {
    switch (having->type) {
      case T_A_Expr: {
        result->having = AExprTransform(reinterpret_cast<A_Expr*>(having));
        break;
      }
      case T_BoolExpr: {
        result->having = BoolExprTransform(reinterpret_cast<BoolExpr*>(having));
        break;
      }
      default: {
        LOG_ERROR("HAVING of type %d not supported yet...", having->type);
        delete result;
        throw NotImplementedException("...");
      }
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
      switch (target->type) {
        case T_ColumnRef:
          exprs->push_back(
              ColumnRefTransform(reinterpret_cast<ColumnRef*>(target)));
          break;
        case T_FuncCall:
          exprs->push_back(
              FuncCallTransform(reinterpret_cast<FuncCall*>(target)));
          break;
        case T_A_Expr:
          exprs->push_back(AExprTransform(reinterpret_cast<A_Expr*>(target)));
          break;
        default:
          LOG_ERROR("SortBy type %d not supported...", target->type);
          throw new NotImplementedException("...");
      }
    } else {
      LOG_ERROR("ORDER BY list member type %d\n", temp->type);
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
          type::ValueFactory::GetNullValueByType(type::Type::TypeId::INTEGER));
      break;
    default:
      LOG_ERROR("Value type %d not supported yet...\n", val.type);
  }
  return result;
}

// This function takes in a Posgres A_Const parsenode and transfers it into
// a Peloton constant value expression.
expression::AbstractExpression* PostgresParser::ConstTransform(A_Const* root) {
  return ValueTransform(root->val);
}

// This function takes in a Postgres FuncCall parsenode and transfers it into
// a Peloton FunctionExpression object.
// TODO: support function calls on a single column.
expression::AbstractExpression* PostgresParser::FuncCallTransform(
    FuncCall* root) {
  expression::AbstractExpression* result = nullptr;
  std::string type_string =
      (reinterpret_cast<value*>(root->funcname->head->data.ptr_value))->val.str;

  type_string = "AGGREGATE_" + type_string;

  if (root->agg_star) {
    expression::AbstractExpression* children = new expression::StarExpression();
    result = new expression::AggregateExpression(
        StringToExpressionType(type_string), false, children);
  } else {
    if (root->args->length < 2) {
      // auto children_expr_list = TargetTransform(root->args);
      expression::AbstractExpression* child;
      auto expr_node = (Node*)root->args->head->data.ptr_value;
      if (expr_node->type == T_A_Expr) {
        child = AExprTransform((A_Expr*)expr_node);
      } else if (expr_node->type == T_A_Const) {
        child = ConstTransform((A_Const*)expr_node);
      } else if (expr_node->type == T_ColumnRef) {
        child = ColumnRefTransform((ColumnRef*)expr_node);
      } else {
        LOG_ERROR("Function within Aggregate is not supported yet\n");
        throw NotImplementedException("");
      }
      result = new expression::AggregateExpression(
          StringToExpressionType(type_string), root->agg_distinct, child);
    } else {
      LOG_ERROR("Aggregation over multiple columns not supported yet...\n");
      return nullptr;
    }
  }
  return result;
}

// This function takes in the whereClause part of a Postgres SelectStmt
// parsenode and transfers it into the select_list of a Peloton SelectStatement.
// It checks the type of each target and call the corresponding helpers.
// TODO: Add support for CaseExpr.
std::vector<expression::AbstractExpression*>* PostgresParser::TargetTransform(
    List* root) {
  std::vector<expression::AbstractExpression*>* result =
      new std::vector<expression::AbstractExpression*>();
  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    ResTarget* target = reinterpret_cast<ResTarget*>(cell->data.ptr_value);
    expression::AbstractExpression* expr;
    switch (target->val->type) {
      case T_ColumnRef: {
        expr = ColumnRefTransform(reinterpret_cast<ColumnRef*>(target->val));
        break;
      }
      case T_A_Const: {
        expr = ConstTransform(reinterpret_cast<A_Const*>(target->val));
        break;
      }
      case T_FuncCall: {
        expr = FuncCallTransform(reinterpret_cast<FuncCall*>(target->val));
        break;
      }
      case T_A_Expr: {
        expr = AExprTransform(reinterpret_cast<A_Expr*>(target->val));
        break;
      }
      default: {
        LOG_ERROR("Target type %d not suported yet...\n", target->val->type);
      }
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
    switch (node->type) {
      case T_BoolExpr: {
        next = BoolExprTransform(reinterpret_cast<BoolExpr*>(node));
        break;
      }
      case T_A_Expr: {
        next = AExprTransform(reinterpret_cast<A_Expr*>(node));
        break;
      }
      case T_ColumnRef: {
        next = ColumnRefTransform(reinterpret_cast<ColumnRef*>(node));
        break;
      }
      case T_ParamRef:
        next = ParamRefTransform(reinterpret_cast<ParamRef*>(node));
        break;
      default: {
        LOG_ERROR("BoolExpr arg type %d not suported yet...\n", node->type);
        return nullptr;
      }
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
  target_type = StringToExpressionType(std::string(name));
  if (target_type == ExpressionType::INVALID) {
    LOG_ERROR("COMPARE type %s not supported yet...\n", name);
    return nullptr;
  }
  expression::AbstractExpression* left_expr = nullptr;
  switch (root->lexpr->type) {
    case T_ColumnRef: {
      left_expr = ColumnRefTransform(reinterpret_cast<ColumnRef*>(root->lexpr));
      break;
    }
    case T_A_Const: {
      left_expr = ConstTransform(reinterpret_cast<A_Const*>(root->lexpr));
      break;
    }
    case T_A_Expr: {
      left_expr = AExprTransform(reinterpret_cast<A_Expr*>(root->lexpr));
      break;
    }
    case T_ParamRef:
      left_expr = ParamRefTransform(reinterpret_cast<ParamRef*>(root->lexpr));
      break;
    default: {
      LOG_ERROR("Left expr of type %d not supported yet...\n",
                root->lexpr->type);
      return nullptr;
    }
  }
  expression::AbstractExpression* right_expr = nullptr;
  switch (root->rexpr->type) {
    case T_ColumnRef: {
      right_expr =
          ColumnRefTransform(reinterpret_cast<ColumnRef*>(root->rexpr));
      break;
    }
    case T_A_Const: {
      right_expr = ConstTransform(reinterpret_cast<A_Const*>(root->rexpr));
      break;
    }
    case T_A_Expr: {
      right_expr = AExprTransform(reinterpret_cast<A_Expr*>(root->rexpr));
      break;
    }
    case T_ParamRef:
      right_expr = ParamRefTransform(reinterpret_cast<ParamRef*>(root->rexpr));
      break;
    default: {
      LOG_ERROR("Right expr of type %d not supported yet...\n",
                root->rexpr->type);
      return nullptr;
    }
  }

  int type_id = static_cast<int>(target_type);
  if (type_id <= 4) {
    result = new expression::OperatorExpression(
        target_type, StringToTypeId("INVALID"), left_expr, right_expr);
  } else if ((10 <= type_id) && (type_id <= 17)) {
    result = new expression::ComparisonExpression(target_type, left_expr,
                                                  right_expr);
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
  switch (root->type) {
    case T_A_Expr: {
      result = AExprTransform(reinterpret_cast<A_Expr*>(root));
      break;
    }
    case T_BoolExpr: {
      result = BoolExprTransform(reinterpret_cast<BoolExpr*>(root));
      break;
    }
    default: { LOG_ERROR("WHERE of type %d not supported yet...", root->type); }
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
    LOG_ERROR("Column DataType %s not supported yet...\n", name);
    throw NotImplementedException("...");
  }

  // Transform Varchar len
  result = new ColumnDefinition(cstrdup(root->colname), data_type);
  if (type_name->typmods) {
    Node* node =
        reinterpret_cast<Node*>(type_name->typmods->head->data.ptr_value);
    if (node->type == T_A_Const) {
      if (reinterpret_cast<A_Const*>(node)->val.type != T_Integer) {
        LOG_ERROR("typmods of type %d not supported yet...\n",
                  reinterpret_cast<A_Const*>(node)->val.type);
        delete result;
        throw NotImplementedException("...");
      }
      result->varlen =
          static_cast<size_t>(reinterpret_cast<A_Const*>(node)->val.val.ival);
    } else {
      LOG_ERROR("typmods of type %d not supported yet...\n", node->type);
      delete result;
      throw NotImplementedException("...");
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
      temp->table_info_ = new parser::TableInfo();
      if (relation->relname) {
        temp->table_info_->table_name = cstrdup(relation->relname);
      }
      if (relation->catalogname) {
        temp->table_info_->database_name = cstrdup(relation->catalogname);
      };
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
        result->columns->push_back(col);
      } else {
        LOG_ERROR("Constraint of type %d not supported yet", node->type);
        throw NotImplementedException("");
      }
    } else {
      LOG_ERROR("tableElt of type %d not supported yet...", node->type);
      delete result->table_info_;
      delete result;
      throw NotImplementedException(".");
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
      result->select_list = TargetTransform(root->targetList);
      result->from_table = FromTransform(root->fromClause);
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
      LOG_ERROR("Set operation %d not supported yet...\n", root->op);
      throw NotImplementedException("...");
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
    LOG_ERROR("Commmand type %d not supported yet.\n", root->kind);
    throw NotImplementedException("");
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
    default: {
      LOG_ERROR("Statement of type %d not supported yet...\n", stmt->type);
      throw NotImplementedException("...");
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
  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    result->AddStatement(NodeTransform((Node*)cell->data.ptr_value));
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
    switch (target->val->type) {
      case T_ColumnRef: {
        update_clause->value =
            ColumnRefTransform(reinterpret_cast<ColumnRef*>(target->val));
        break;
      }
      case T_A_Const: {
        update_clause->value =
            ConstTransform(reinterpret_cast<A_Const*>(target->val));
        break;
      }
      case T_FuncCall: {
        update_clause->value =
            FuncCallTransform(reinterpret_cast<FuncCall*>(target->val));
        break;
      }
      case T_A_Expr: {
        update_clause->value =
            AExprTransform(reinterpret_cast<A_Expr*>(target->val));
        break;
      }
      case T_ParamRef:
        update_clause->value =
            ParamRefTransform(reinterpret_cast<ParamRef*>(target->val));
        break;
      default: {
        LOG_ERROR("Target type %d not suported yet...\n", target->val->type);
      }
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
parser::SQLStatementList* PostgresParser::ParseSQLString(
    const char* text) {
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
//   print_pg_parse_tree(result.tree);

  auto transform_result = ListTransform(result.tree);
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
  return std::move(sql_stmt);
}

}  // End pgparser namespace
}  // End peloton namespace
