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

std::string ParserUtils::GetTableRefInfo(const TableRef* table,
                                         uint num_indent) {
  std::ostringstream os;
  switch (table->type) {
    case TableReferenceType::NAME:
      os << StringUtil::Indent(num_indent) << table->GetTableName() << "\n";
      break;

    case TableReferenceType::SELECT:
      os << GetSelectStatementInfo(table->select, num_indent) << "\n";
      break;

    case TableReferenceType::JOIN:
      os << StringUtil::Indent(num_indent) << "-> Join Table\n";
      os << StringUtil::Indent(num_indent + 1) + "-> Left\n";
      os << GetTableRefInfo(table->join->left.get(), num_indent + 2) << "\n";
      os << StringUtil::Indent(num_indent + 1) << "-> Right\n";
      os << GetTableRefInfo(table->join->right.get(), num_indent + 2) << "\n";
      os << StringUtil::Indent(num_indent + 1) << "-> Join Condition\n";
      os << GetExpressionInfo(table->join->condition.get(), num_indent + 2) << "\n";
      break;

    case TableReferenceType::CROSS_PRODUCT:
      for (auto &tbl : table->list) {
        os << GetTableRefInfo(tbl.get(), num_indent + 1) << "\n";
      }
      break;

    case TableReferenceType::INVALID:
    default:
      LOG_ERROR("invalid table ref type");
      break;
  }

  if (!table->alias.empty()) {
    os << StringUtil::Indent(num_indent) << "Alias: " << table->alias;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

std::string ParserUtils::GetOperatorExpression(
    const expression::AbstractExpression* expr, uint num_indent) {
  if (expr == NULL) {
    return StringUtil::Indent(num_indent) + "null";
  }

  std::ostringstream os;
  os << GetExpressionInfo(expr->GetChild(0), num_indent);
  if (expr->GetChild(1) != NULL)
    os << "\n" << GetExpressionInfo(expr->GetChild(1), num_indent);
  return os.str();
}

std::string ParserUtils::GetExpressionInfo(
    const expression::AbstractExpression* expr, uint num_indent) {
  if (expr == NULL) {
    return StringUtil::Indent(num_indent) + "null";
  }

  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "-> Expr Type :: "
     << ExpressionTypeToString(expr->GetExpressionType()) << "\n";

  switch (expr->GetExpressionType()) {
    case ExpressionType::STAR:
      os << StringUtil::Indent(num_indent + 1) << "*\n";
      break;
    case ExpressionType::VALUE_TUPLE:
      os << StringUtil::Indent(num_indent + 1) << expr->GetInfo() << "\n";
      os << StringUtil::Indent(num_indent + 1)
         << ((expression::TupleValueExpression*)expr)->GetTableName() << "<";
      os << ((expression::TupleValueExpression*)expr)->GetColumnName() << ">\n";
      break;
    case ExpressionType::COMPARE_GREATERTHAN:
      os << StringUtil::Indent(num_indent + 1) << expr->GetInfo() << "\n";
      for (size_t i = 0; i < (expr)->GetChildrenSize(); ++i) {
        os << StringUtil::Indent(num_indent + 1) << ((expr)->GetChild(i))->GetInfo() << "\n";
      }
      break;
    case ExpressionType::VALUE_CONSTANT:
      os << StringUtil::Indent(num_indent + 1) << expr->GetInfo() << "\n";
      break;
    case ExpressionType::FUNCTION_REF:
      os << StringUtil::Indent(num_indent + 1) << expr->GetInfo() << "\n";
      break;
    default:
      os << GetOperatorExpression(expr, num_indent + 1);
      break;
  }

  // TODO: Fix this
  if (expr->alias.size() != 0) {
    os << StringUtil::Indent(num_indent) << "Alias: " << expr->alias;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

std::string ParserUtils::GetSelectStatementInfo(SelectStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "SelectStatement\n";
  os << StringUtil::Indent(num_indent + 1) << "-> Fields:\n";
  for (auto &expr : stmt->select_list)
    os << GetExpressionInfo(expr.get(), num_indent + 2) << std::endl;

  if (stmt->from_table != NULL) {
    os << StringUtil::Indent(num_indent + 1) + "-> Sources:\n";
    os << GetTableRefInfo(stmt->from_table.get(), num_indent + 2) << std::endl;
  }

  if (stmt->where_clause != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> Search Conditions:\n";
    os << GetExpressionInfo(stmt->where_clause.get(), num_indent + 2) << std::endl;
  }

  if (stmt->union_select != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> Union:\n";
    os << GetSelectStatementInfo(stmt->union_select.get(), num_indent + 2) << std::endl;
  }

  if (stmt->order != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> OrderBy:\n";
    for (size_t idx = 0; idx < stmt->order->exprs.size(); idx++) {
      auto &expr = stmt->order->exprs.at(idx);
      auto &type = stmt->order->types.at(idx);
      os << GetExpressionInfo(expr.get(), num_indent + 2) << std::endl;
      if (type == kOrderAsc)
        os << StringUtil::Indent(num_indent + 2) << "ascending\n";
      else
        os << StringUtil::Indent(num_indent + 2) << "descending\n";
    }
  }

  if (stmt->group_by != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> GroupBy:\n";
    for (auto &column : stmt->group_by->columns) {
      os << StringUtil::Indent(num_indent + 2) << column->GetInfo() << std::endl;
    }
    if (stmt->group_by->having) {
      os << StringUtil::Indent(num_indent + 2) << stmt->group_by->having->GetInfo() << std::endl;
    }
  }

  if (stmt->limit != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> Limit:\n";
    os << StringUtil::Indent(num_indent + 2) << std::to_string(stmt->limit->limit) << "\n";
    os << StringUtil::Indent(num_indent + 2) << std::to_string(stmt->limit->offset) << "\n";
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

std::string ParserUtils::GetCreateStatementInfo(CreateStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "CreateStatment\n";
  os << StringUtil::Indent(num_indent + 1) << stmt->type << "\n";

  if (stmt->type == CreateStatement::CreateType::kIndex) {
    os << StringUtil::Indent(num_indent + 1) << stmt->index_name << "\n";
    os << StringUtil::Indent(num_indent + 1) << "INDEX : table : " << stmt->GetTableName()
       << " unique : " << stmt->unique << " attrs : ";
    for (auto &key : stmt->index_attrs) os << key << " ";
    os << "\n";
  } else if (stmt->type == CreateStatement::CreateType::kTable) {
    os << StringUtil::Indent(num_indent + 1) << stmt->GetTableName() << "\n";
  }

  if (!stmt->columns.empty()) {
    for (auto &col : stmt->columns) {
      if (col->name.empty()) {continue;}
      if (col->type == ColumnDefinition::DataType::PRIMARY) {
        os << StringUtil::Indent(num_indent + 1) << "-> PRIMARY KEY : ";
        for (auto &key : col->primary_key) os << key << " ";
        os << "\n";
      } else if (col->type == ColumnDefinition::DataType::FOREIGN) {
        os << StringUtil::Indent(num_indent + 1) << "-> FOREIGN KEY : References " << col->name << " Source : ";
        for (auto &key : col->foreign_key_source) {
          os << key << " ";
        }
        os << "Sink : ";
        for (auto &key : col->foreign_key_sink) {
          os << key << " ";
        }
        os << "\n";
      } else {
        os << StringUtil::Indent(num_indent + 1) << "-> COLUMN REF : " << col->name << " "
           // << col->type << " not null : "
           << col->not_null << " primary : "
           << col->primary << " unique " << col->unique << " varlen "
           << col->varlen << "\n";
      }
    }
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

std::string ParserUtils::GetInsertStatementInfo(InsertStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "InsertStatment\n";
  os << StringUtil::Indent(num_indent + 1) << stmt->GetTableName() << std::endl;
  if (!stmt->columns.empty()) {
    os << StringUtil::Indent(num_indent + 1) << "-> Columns\n";
    for (auto &col_name : stmt->columns) {
      os << StringUtil::Indent(num_indent + 2) << col_name << std::endl;
    }
  }
  switch (stmt->type) {
    case InsertType::VALUES:
      os << StringUtil::Indent(num_indent + 1) << "-> Values\n";
      for (auto &value_item : stmt->insert_values) {
        // TODO this is a debugging method which is currently unused.
        for (auto &expr : value_item) {
          os << GetExpressionInfo(expr.get(), num_indent + 2) << std::endl;
        }
      }
      break;
    case InsertType::SELECT:
      os << GetSelectStatementInfo(stmt->select.get(), num_indent + 1);
      break;
    default:
      break;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

std::string ParserUtils::GetDeleteStatementInfo(DeleteStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "DeleteStatment\n";
  os << StringUtil::Indent(num_indent + 1) << stmt->GetTableName();
  return os.str();
}

std::string ParserUtils::GetUpdateStatementInfo(UpdateStatement* stmt,
                                                uint num_indent) {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "UpdateStatment\n";
  os << GetTableRefInfo(stmt->table.get(), num_indent + 1) << std::endl;
  os << StringUtil::Indent(num_indent + 1) << "-> Updates :: \n";
  for (auto &update : stmt->updates) {
    os << StringUtil::Indent(num_indent + 2) << "Column: " << update->column << std::endl;
    os << GetExpressionInfo(update->value.get(), num_indent + 3) << std::endl;
  }
  os << StringUtil::Indent(num_indent + 1) << "-> Where :: \n"
     << GetExpressionInfo(stmt->where.get(), num_indent + 2);
  return os.str();
}

std::string ParserUtils::GetCopyStatementInfo(CopyStatement* stmt,
                                              uint num_indent) {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "CopyStatment\n";
  os << StringUtil::Indent(num_indent + 1) << "-> Type :: " << CopyTypeToString(stmt->type)
     << "\n";
  os << GetTableRefInfo(stmt->cpy_table.get(), num_indent + 1) << std::endl;

  os << StringUtil::Indent(num_indent + 1) << "-> File Path :: " << stmt->file_path << std::endl;
  os << StringUtil::Indent(num_indent + 1) << "-> Delimiter :: " << stmt->delimiter;
  return os.str();
}

}  // namespace parser
}  // namespace peloton
