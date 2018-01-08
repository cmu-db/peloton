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

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

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
