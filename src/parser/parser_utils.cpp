//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_utils.cpp
//
// Identification: src/parser/parser_utils.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/parser_utils.h"
#include "type/types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "expression/tuple_value_expression.h"

#include <stdio.h>
#include <string>
#include <iostream>

namespace peloton {
namespace parser {

std::string indent(uint num_indent) { return std::string(num_indent, '\t'); }

std::string ParserUtils::GetTableRefInfo(const TableRef* table,
                                         uint num_indent) {
  std::string output;
  switch (table->type) {
    case TableReferenceType::NAME:
      output += indent(num_indent) + table->GetTableName() + "\n";
      break;

    case TableReferenceType::SELECT:
      output += GetSelectStatementInfo(table->select, num_indent) + "\n";
      break;

    case TableReferenceType::JOIN:
      output += indent(num_indent) + "-> Join Table\n";
      output += indent(num_indent + 1) + "-> Left\n";
      output += GetTableRefInfo(table->join->left, num_indent + 2) + "\n";
      output += indent(num_indent + 1) + "-> Right\n";
      output += GetTableRefInfo(table->join->right, num_indent + 2) + "\n";
      output += indent(num_indent + 1) + "-> Join Condition\n";
      output +=
          GetExpressionInfo(table->join->condition, num_indent + 2) + "\n";
      break;

    case TableReferenceType::CROSS_PRODUCT:
      for (TableRef* tbl : *table->list) {
        output += GetTableRefInfo(tbl, num_indent) + "\n";
      }
      break;

    case TableReferenceType::INVALID:
    default:
      LOG_ERROR("invalid table ref type");
      break;
  }

  if (table->alias != NULL) {
    output += indent(num_indent + 1) + "Alias\n";
    output += indent(num_indent + 2) + table->alias + "\n";
  }
  return output;
}

std::string ParserUtils::GetOperatorExpression(
    const expression::AbstractExpression* expr, uint num_indent) {
  if (expr == NULL) {
    return indent(num_indent) + "null\n";
  }

  std::string output =
      GetExpressionInfo(expr->GetChild(0), num_indent + 1) + "\n";
  if (expr->GetChild(1) != NULL)
    output += GetExpressionInfo(expr->GetChild(1), num_indent + 1) + "\n";
  return output;
}

std::string ParserUtils::GetExpressionInfo(
    const expression::AbstractExpression* expr, uint num_indent) {
  if (expr == NULL) {
    return indent(num_indent) + "null\n";
  }

  std::string output = indent(num_indent) + "-> Expr Type :: " +
                       ExpressionTypeToString(expr->GetExpressionType()) + "\n";

  switch (expr->GetExpressionType()) {
    case ExpressionType::STAR:
      output += indent(num_indent) + "*\n";
      break;
    case ExpressionType::VALUE_TUPLE:
      output += indent(num_indent) + expr->GetInfo() + "\n";
      output += indent(num_indent) +
                ((expression::TupleValueExpression*)expr)->GetTableName() +
                "\n";
      output += indent(num_indent) +
                ((expression::TupleValueExpression*)expr)->GetColumnName() +
                "\n";
      break;
    case ExpressionType::COMPARE_GREATERTHAN:
      output += indent(num_indent) + expr->GetInfo() + "\n";
      for (size_t i = 0; i < (expr)->GetChildrenSize(); ++i) {
        output += indent(num_indent) + ((expr)->GetChild(i))->GetInfo() + "\n";
      }
      break;
    case ExpressionType::VALUE_CONSTANT:
      output += indent(num_indent) + expr->GetInfo() + "\n";
      break;
    case ExpressionType::FUNCTION_REF:
      output += indent(num_indent) + expr->GetInfo() + "\n";
      break;
    default:
      output += GetOperatorExpression(expr, num_indent);
      break;
  }

  // TODO: Fix this
  if (expr->alias.size() != 0) {
    output += indent(num_indent + 1) + "Alias\n";
    output += indent(num_indent + 2) + expr->alias;
  }
  return output;
}

std::string ParserUtils::GetSelectStatementInfo(SelectStatement* stmt,
                                                uint num_indent) {
  std::string output;
  output += indent(num_indent) + "SelectStatement\n";
  output += indent(num_indent + 1) + "-> Fields:\n";
  for (expression::AbstractExpression* expr : *(stmt->select_list))
    output += GetExpressionInfo(expr, num_indent + 2);

  output += indent(num_indent + 1) + "-> Sources:\n";
  if (stmt->from_table != NULL) {
    output += GetTableRefInfo(stmt->from_table, num_indent + 2);
  }

  if (stmt->where_clause != NULL) {
    output += indent(num_indent + 1) + "-> Search Conditions:\n";
    output += GetExpressionInfo(stmt->where_clause, num_indent + 2);
  }

  if (stmt->union_select != NULL) {
    output += indent(num_indent + 1) + "-> Union:\n";
    output += GetSelectStatementInfo(stmt->union_select, num_indent + 2);
  }

  if (stmt->order != NULL) {
    output += indent(num_indent + 1) + "-> OrderBy:\n";
    for (size_t idx = 0; idx < stmt->order->exprs->size(); idx++) {
      auto expr = stmt->order->exprs->at(idx);
      auto type = stmt->order->types->at(idx);
      output += GetExpressionInfo(expr, num_indent + 2);
      if (type == kOrderAsc)
        output += indent(num_indent + 2) + "ascending\n";
      else
        output += indent(num_indent + 2) + "descending\n";
    }
  }

  if (stmt->group_by != NULL) {
    output += indent(num_indent + 1) + "-> GroupBy:\n";
    for (auto column : *(stmt->group_by->columns)) {
      output += indent(num_indent + 2) + column->GetInfo() + "\n";
    }
    if (stmt->group_by->having) {
      output +=
          indent(num_indent + 2) + stmt->group_by->having->GetInfo() + "\n";
    }
  }

  if (stmt->limit != NULL) {
    output += indent(num_indent + 1) + "-> Limit:\n";
    output +=
        indent(num_indent + 2) + std::to_string(stmt->limit->limit) + "\n";
    output +=
        indent(num_indent + 2) + std::to_string(stmt->limit->offset) + "\n";
  }
  return output;
}

std::string ParserUtils::GetCreateStatementInfo(CreateStatement* stmt,
                                                uint num_indent) {
  std::string output = indent(num_indent) + "CreateStatment\n";
  output += indent(num_indent + 1) + std::to_string(stmt->type) + "\n";

  if (stmt->type == CreateStatement::CreateType::kIndex) {
    output += indent(num_indent + 1) + stmt->index_name + "\n";
    output += indent(num_indent) + "INDEX : table : " + stmt->GetTableName() +
              " unique : " + std::to_string(stmt->unique) + " attrs : ";
    for (auto key : *(stmt->index_attrs)) output += std::string(key) + " ";
    output += "\n";
  } else if (stmt->type == CreateStatement::CreateType::kTable) {
    output += indent(num_indent + 1) + stmt->GetTableName() + "\n";
  }

  if (stmt->columns != nullptr) {
    for (ColumnDefinition* col : *(stmt->columns)) {
      if (col->name == nullptr) {continue;}
      output += indent(num_indent);
      if (col->type == ColumnDefinition::DataType::PRIMARY) {
        output += "-> PRIMARY KEY : ";
        for (auto key : *(col->primary_key)) output += std::string(key) + " ";
        output += "\n";
      } else if (col->type == ColumnDefinition::DataType::FOREIGN) {
        output += "-> FOREIGN KEY : References " + std::string(col->name) +
                  " Source : ";
        for (auto key : *(col->foreign_key_source)) {
          output += std::string(key) + " ";
        }
        output += "Sink : ";
        for (auto key : *(col->foreign_key_sink)) {
          output += std::string(key) + " ";
        }
        output += "\n";
      } else {
        output += "-> COLUMN REF : " + std::string(col->name) + " " +
                  std::to_string(col->type) + " not null : " +
                  std::to_string(col->not_null) + " primary : " +
                  std::to_string(col->primary) + " unique " +
                  std::to_string(col->unique) + " varlen " +
                  std::to_string(col->varlen) + "\n";
      }
    }
  }
  return output;
}

std::string ParserUtils::GetInsertStatementInfo(InsertStatement* stmt,
                                                uint num_indent) {
  std::string output;
  output += indent(num_indent) + "InsertStatment\n";
  output += indent(num_indent + 1) + stmt->GetTableName() + "\n";
  if (stmt->columns != NULL) {
    output += indent(num_indent + 1) + "-> Columns\n";
    for (char* col_name : *stmt->columns) {
      output += indent(num_indent + 2) + col_name + "\n";
    }
  }
  switch (stmt->type) {
    case InsertType::VALUES:
      output += indent(num_indent + 1) + "-> Values\n";
      for (auto value_item : *stmt->insert_values) {
        // TODO this is a debugging method which is currently unused.
        for (expression::AbstractExpression* expr : *value_item) {
          output += GetExpressionInfo(expr, num_indent + 2);
        }
      }
      break;
    case InsertType::SELECT:
      output += GetSelectStatementInfo(stmt->select, num_indent + 1);
      break;
    default:
      break;
  }
  return output;
}

std::string ParserUtils::GetDeleteStatementInfo(DeleteStatement* stmt,
                                                uint num_indent) {
  std::string output = indent(num_indent) + "DeleteStatment\n";
  output += indent(num_indent + 1) + stmt->GetTableName() + "\n";
  return output;
}

std::string ParserUtils::GetUpdateStatementInfo(UpdateStatement* stmt,
                                                uint num_indent) {
  std::string output = indent(num_indent) + "UpdateStatment\n";
  output += GetTableRefInfo(stmt->table, num_indent + 1);
  output += indent(num_indent) + "-> Updates :: \n";
  for (UpdateClause* update : *(stmt->updates)) {
    output += indent(num_indent + 1) + "Column: " +
              std::string(update->column) + "\n";
    output += GetExpressionInfo(update->value, num_indent + 1);
  }
  output += indent(num_indent) + "-> Where :: " +
            GetExpressionInfo(stmt->where, num_indent + 1);
  return output;
}

std::string ParserUtils::GetCopyStatementInfo(CopyStatement* stmt,
                                              uint num_indent) {
  std::string output = indent(num_indent) + "CopyStatment\n";
  output +=
      indent(num_indent) + "-> Type :: " + CopyTypeToString(stmt->type) + "\n";
  output += GetTableRefInfo(stmt->cpy_table, num_indent + 1);

  output += indent(num_indent) + "-> File Path :: " +
            std::string(stmt->file_path) + "\n";
  output += indent(num_indent) + "-> Delimiter :: " + stmt->delimiter + "\n";
  return output;
}

}  // namespace parser
}  // namespace peloton
