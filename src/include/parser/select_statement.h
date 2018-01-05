//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_select.h
//
// Identification: src/include/parser/statement_select.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"
#include "parser/parser_utils.h"
#include "util/string_util.h"
#include <vector>

namespace peloton {
namespace parser {

/**
 * @class OrderDescription
 * @brief Description of the order by clause within a select statement
 *
 * TODO: hold multiple expressions to be sorted by
 */
typedef enum { kOrderAsc, kOrderDesc } OrderType;

class OrderDescription {
 public:
  OrderDescription() {}

  virtual ~OrderDescription() {}

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  std::vector<OrderType> types;
  std::vector<std::unique_ptr<expression::AbstractExpression>> exprs;
};

/**
 * @class LimitDescription
 * @brief Description of the limit clause within a select statement
 */
const int64_t kNoLimit = -1;
const int64_t kNoOffset = -1;
class LimitDescription {
 public:
  LimitDescription(int64_t limit, int64_t offset)
      : limit(limit), offset(offset) {}

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  int64_t limit;
  int64_t offset;
};

/**
 * @class GroupByDescription
 */
class GroupByDescription {
 public:
  GroupByDescription() : having(nullptr) {}

  ~GroupByDescription() {}

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  std::vector<std::unique_ptr<expression::AbstractExpression>> columns;
  std::unique_ptr<expression::AbstractExpression> having;
};

/**
 * @struct SelectStatement
 * @brief Representation of a full select statement.
 *
 * TODO: add union_order and union_limit
 */
class SelectStatement : public SQLStatement {
 public:
  SelectStatement()
      : SQLStatement(StatementType::SELECT),
        from_table(nullptr),
        select_distinct(false),
        where_clause(nullptr),
        group_by(nullptr),
        union_select(nullptr),
        order(nullptr),
        limit(nullptr),
        is_for_update(false){};

  virtual ~SelectStatement() {}

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const {
    std::ostringstream os;
    os << StringUtil::Indent(num_indent) << "SelectStatement\n";
    os << StringUtil::Indent(num_indent + 1) << "-> Fields:\n";
    for (auto &expr : select_list) os << expr.get()->GetInfo() << std::endl;

    if (from_table != NULL) {
      os << StringUtil::Indent(num_indent + 1) + "-> Sources:\n";
      os << from_table.get()->GetInfo(num_indent + 2) << std::endl;
    }

    if (where_clause != NULL) {
      os << StringUtil::Indent(num_indent + 1) << "-> Search Conditions:\n";
      os << where_clause.get()->GetInfo() << std::endl;
    }

    if (union_select != NULL) {
      os << StringUtil::Indent(num_indent + 1) << "-> Union:\n";
      os << union_select.get()->GetInfo(num_indent + 2) << std::endl;
    }

    if (order != NULL) {
      os << StringUtil::Indent(num_indent + 1) << "-> OrderBy:\n";
      for (size_t idx = 0; idx < order->exprs.size(); idx++) {
        auto &expr = order->exprs.at(idx);
        auto &type = order->types.at(idx);
        os << expr.get()->GetInfo() << std::endl;
        if (type == kOrderAsc)
          os << StringUtil::Indent(num_indent + 2) << "ascending\n";
        else
          os << StringUtil::Indent(num_indent + 2) << "descending\n";
      }
    }

    if (group_by != NULL) {
      os << StringUtil::Indent(num_indent + 1) << "-> GroupBy:\n";
      for (auto &column : group_by->columns) {
        os << StringUtil::Indent(num_indent + 2) << column->GetInfo()
           << std::endl;
      }
      if (group_by->having) {
        os << StringUtil::Indent(num_indent + 2) << group_by->having->GetInfo()
           << std::endl;
      }
    }

    if (limit != NULL) {
      os << StringUtil::Indent(num_indent + 1) << "-> Limit:\n";
      os << StringUtil::Indent(num_indent + 2) << std::to_string(limit->limit)
         << "\n";
      os << StringUtil::Indent(num_indent + 2) << std::to_string(limit->offset)
         << "\n";
    }
    std::string info = os.str();
    StringUtil::RTrim(info);
    return info;
  }

  const std::string GetInfo() const {
    std::ostringstream os;

    os << "SQLStatement[SELECT]\n";

    os << GetInfo(1);

    return os.str();
  }

  std::unique_ptr<TableRef> from_table;
  bool select_distinct;
  std::vector<std::unique_ptr<expression::AbstractExpression>> select_list;
  std::unique_ptr<expression::AbstractExpression> where_clause;
  std::unique_ptr<GroupByDescription> group_by;

  std::unique_ptr<SelectStatement> union_select;
  std::unique_ptr<OrderDescription> order;
  std::unique_ptr<LimitDescription> limit;
  bool is_for_update;
  int depth = -1;

 public:
  const std::vector<std::unique_ptr<expression::AbstractExpression>> &
  getSelectList() const {
    return select_list;
  }

  void UpdateWhereClause(expression::AbstractExpression *expr) {
    where_clause.reset(expr);
  }
};

}  // namespace parser
}  // namespace peloton
