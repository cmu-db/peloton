//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_utils.cpp
//
// Identification: src/parser/parser_utils.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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
#include <sstream>

namespace peloton {
namespace parser {

std::string ParserUtils::indent(uint num_indent) {
  return std::string(num_indent * 2, ' ');
}

std::string ParserUtils::GetTableRefInfo(const TableRef* table,
                                         uint num_indent) {
  std::ostringstream os;
  switch (table->type) {
    case TableReferenceType::NAME:
      os << indent(num_indent) << table->GetTableName() << "\n";
      break;

    case TableReferenceType::SELECT:
      os << GetSelectStatementInfo(table->select, num_indent) << "\n";
      break;

    case TableReferenceType::JOIN:
      os << indent(num_indent) << "-> Join Table\n";
      os << indent(num_indent + 1) + "-> Left\n";
      os << GetTableRefInfo(table->join->left.get(), num_indent + 2) << "\n";
      os << indent(num_indent + 1) << "-> Right\n";
      os << GetTableRefInfo(table->join->right.get(), num_indent + 2) << "\n";
      os << indent(num_indent + 1) << "-> Join Condition\n";
      os << GetExpressionInfo(table->join->condition.get(), num_indent + 2) << "\n";
      break;

    case TableReferenceType::CROSS_PRODUCT:
      for (auto &tbl : table->list) {
        os << GetTableRefInfo(tbl.get(), num_indent) << "\n";
      }
      break;

    case TableReferenceType::INVALID:
    default:
      LOG_ERROR("invalid table ref type");
      break;
  }

  if (!table->alias.empty()) {
    os << indent(num_indent + 1) << "Alias\n";
    os << indent(num_indent + 2) << table->alias << "\n";
  }
  return os.str();
}

std::string ParserUtils::GetOperatorExpression(
    const expression::AbstractExpression* expr, uint num_indent) {
  if (expr == NULL) {
    return indent(num_indent) + "null\n";
  }

  std::ostringstream os;
  os << GetExpressionInfo(expr->GetChild(0), num_indent + 1) << "\n";
  if (expr->GetChild(1) != NULL)
    os << GetExpressionInfo(expr->GetChild(1), num_indent + 1) << "\n";
  return os.str();
}

std::string ParserUtils::GetExpressionInfo(
    const expression::AbstractExpression* expr, uint num_indent) {
  if (expr == NULL) {
    return indent(num_indent) + "null\n";
  }

  std::ostringstream os;
  os << indent(num_indent) << "-> Expr Type :: "
     << ExpressionTypeToString(expr->GetExpressionType()) << "\n";

  switch (expr->GetExpressionType()) {
    case ExpressionType::STAR:
      os << indent(num_indent) << "*\n";
      break;
    case ExpressionType::VALUE_TUPLE:
      os << indent(num_indent) << expr->GetInfo() << "\n";
      os << indent(num_indent)
         << ((expression::TupleValueExpression*)expr)->GetTableName() << "\n";
      os << indent(num_indent)
         << ((expression::TupleValueExpression*)expr)->GetColumnName() << "\n";
      break;
    case ExpressionType::COMPARE_GREATERTHAN:
      os << indent(num_indent) << expr->GetInfo() << "\n";
      for (size_t i = 0; i < (expr)->GetChildrenSize(); ++i) {
        os << indent(num_indent) << ((expr)->GetChild(i))->GetInfo() << "\n";
      }
      break;
    case ExpressionType::VALUE_CONSTANT:
      os << indent(num_indent) << expr->GetInfo() << "\n";
      break;
    case ExpressionType::FUNCTION_REF:
      os << indent(num_indent) << expr->GetInfo() << "\n";
      break;
    default:
      os << GetOperatorExpression(expr, num_indent);
      break;
  }

  // TODO: Fix this
  if (expr->alias.size() != 0) {
    os << indent(num_indent + 1) << "Alias\n";
    os << indent(num_indent + 2) << expr->alias;
  }
  return os.str();
}

std::string ParserUtils::GetSelectStatementInfo(SelectStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << indent(num_indent) << "SelectStatement\n";
  os << indent(num_indent + 1) << "-> Fields:\n";
  for (auto &expr : stmt->select_list)
    os << GetExpressionInfo(expr.get(), num_indent + 2);

  os << indent(num_indent + 1) + "-> Sources:\n";
  if (stmt->from_table != NULL) {
    os << GetTableRefInfo(stmt->from_table.get(), num_indent + 2);
  }

  if (stmt->where_clause != NULL) {
    os << indent(num_indent + 1) << "-> Search Conditions:\n";
    os << GetExpressionInfo(stmt->where_clause.get(), num_indent + 2);
  }

  if (stmt->union_select != NULL) {
    os << indent(num_indent + 1) << "-> Union:\n";
    os << GetSelectStatementInfo(stmt->union_select.get(), num_indent + 2);
  }

  if (stmt->order != NULL) {
    os << indent(num_indent + 1) << "-> OrderBy:\n";
    for (size_t idx = 0; idx < stmt->order->exprs.size(); idx++) {
      auto &expr = stmt->order->exprs.at(idx);
      auto &type = stmt->order->types.at(idx);
      os << GetExpressionInfo(expr.get(), num_indent + 2);
      if (type == kOrderAsc)
        os << indent(num_indent + 2) << "ascending\n";
      else
        os << indent(num_indent + 2) << "descending\n";
    }
  }

  if (stmt->group_by != NULL) {
    os << indent(num_indent + 1) << "-> GroupBy:\n";
    for (auto &column : stmt->group_by->columns) {
      os << indent(num_indent + 2) << column->GetInfo() << "\n";
    }
    if (stmt->group_by->having) {
      os << indent(num_indent + 2) << stmt->group_by->having->GetInfo() << "\n";
    }
  }

  if (stmt->limit != NULL) {
    os << indent(num_indent + 1) << "-> Limit:\n";
    os << indent(num_indent + 2) << std::to_string(stmt->limit->limit) << "\n";
    os << indent(num_indent + 2) << std::to_string(stmt->limit->offset) << "\n";
  }
  return os.str();
}

std::string ParserUtils::GetCreateStatementInfo(CreateStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << indent(num_indent) << "CreateStatment\n";
  os << indent(num_indent + 1) << stmt->type << "\n";

  if (stmt->type == CreateStatement::CreateType::kIndex) {
    os << indent(num_indent + 1) << stmt->index_name << "\n";
    os << indent(num_indent) << "INDEX : table : " << stmt->GetTableName()
       << " unique : " << stmt->unique << " attrs : ";
    for (auto &key : stmt->index_attrs) os << key << " ";
    os << "\n";
  } else if (stmt->type == CreateStatement::CreateType::kTable) {
    os << indent(num_indent + 1) << stmt->GetTableName() << "\n";
  }

  if (!stmt->columns.empty()) {
    for (auto &col : stmt->columns) {
      if (col->name.empty()) {continue;}
      os << indent(num_indent);
      if (col->type == ColumnDefinition::DataType::PRIMARY) {
        os << "-> PRIMARY KEY : ";
        for (auto &key : col->primary_key) os << key << " ";
        os << "\n";
      } else if (col->type == ColumnDefinition::DataType::FOREIGN) {
        os << "-> FOREIGN KEY : References " << col->name << " Source : ";
        for (auto &key : col->foreign_key_source) {
          os << key << " ";
        }
        os << "Sink : ";
        for (auto &key : col->foreign_key_sink) {
          os << key << " ";
        }
        os << "\n";
      } else {
        os << "-> COLUMN REF : " << col->name << " "
           // << col->type << " not null : "
           << col->not_null << " primary : "
           << col->primary << " unique " << col->unique << " varlen "
           << col->varlen << "\n";
      }
    }
  }
  return os.str();
}

std::string ParserUtils::GetInsertStatementInfo(InsertStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << indent(num_indent) << "InsertStatment\n";
  os << indent(num_indent + 1) << stmt->GetTableName() + "\n";
  if (!stmt->columns.empty()) {
    os << indent(num_indent + 1) << "-> Columns\n";
    for (auto &col_name : stmt->columns) {
      os << indent(num_indent + 2) << col_name << "\n";
    }
  }
  switch (stmt->type) {
    case InsertType::VALUES:
      os << indent(num_indent + 1) << "-> Values\n";
      for (auto &value_item : stmt->insert_values) {
        // TODO this is a debugging method which is currently unused.
        for (auto &expr : value_item) {
          os << GetExpressionInfo(expr.get(), num_indent + 2);
        }
      }
      break;
    case InsertType::SELECT:
      os << GetSelectStatementInfo(stmt->select.get(), num_indent + 1);
      break;
    default:
      break;
  }
  return os.str();
}

std::string ParserUtils::GetDeleteStatementInfo(DeleteStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << indent(num_indent) << "DeleteStatment\n";
  os << indent(num_indent + 1) << stmt->GetTableName() << "\n";
  return os.str();
}

std::string ParserUtils::GetUpdateStatementInfo(UpdateStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << indent(num_indent) << "UpdateStatment\n";
  os << GetTableRefInfo(stmt->table.get(), num_indent + 1);
  os << indent(num_indent) << "-> Updates :: \n";
  for (auto &update : stmt->updates) {
    os << indent(num_indent + 1) << "Column: " << update->column << "\n";
    os << GetExpressionInfo(update->value.get(), num_indent + 1);
  }
  os << indent(num_indent) << "-> Where :: "
     << GetExpressionInfo(stmt->where.get(), num_indent + 1);
  return os.str();
}

std::string ParserUtils::GetCopyStatementInfo(CopyStatement* stmt,
                                              uint num_indent) {
  std::ostringstream os;
  os << indent(num_indent) << "CopyStatment\n";
  os << indent(num_indent) << "-> Type :: " << CopyTypeToString(stmt->type)
     << "\n";
  os << GetTableRefInfo(stmt->cpy_table.get(), num_indent + 1);

  os << indent(num_indent) << "-> File Path :: " << stmt->file_path << "\n";
  os << indent(num_indent) << "-> Delimiter :: " << stmt->delimiter << "\n";
  return os.str();
}

}  // namespace parser
}  // namespace peloton
