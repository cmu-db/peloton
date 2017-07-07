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
#include <vector>

namespace peloton {
namespace parser {

/**
 * @struct OrderDescription
 * @brief Description of the order by clause within a select statement
 *
 * TODO: hold multiple expressions to be sorted by
 */
typedef enum { kOrderAsc, kOrderDesc } OrderType;

struct OrderDescription {
  OrderDescription() {}

  virtual ~OrderDescription() {}

  void Accept(SqlNodeVisitor* v) const { v->Visit(this); }

  std::unique_ptr<std::vector<OrderType>> types;
  std::unique_ptr<std::vector<std::unique_ptr<expression::AbstractExpression>>> exprs;
};

/**
 * @struct LimitDescription
 * @brief Description of the limit clause within a select statement
 */
const int64_t kNoLimit = -1;
const int64_t kNoOffset = -1;
struct LimitDescription {
  LimitDescription(int64_t limit, int64_t offset)
      : limit(limit), offset(offset) {}

  void Accept(SqlNodeVisitor* v) const { v->Visit(this); }

  int64_t limit;
  int64_t offset;
};

/**
 * @struct GroupByDescription
 */
struct GroupByDescription {
  GroupByDescription() : columns(nullptr), having(nullptr) {}

  ~GroupByDescription() {}

  void Accept(SqlNodeVisitor* v) const { v->Visit(this); }

  std::unique_ptr<std::vector<std::unique_ptr<expression::AbstractExpression>>> columns;
  std::unique_ptr<expression::AbstractExpression> having;
};

/**
 * @struct SelectStatement
 * @brief Representation of a full select statement.
 *
 * TODO: add union_order and union_limit
 */
struct SelectStatement : SQLStatement {
  SelectStatement()
      : SQLStatement(StatementType::SELECT),
        from_table(nullptr),
        select_distinct(false),
        select_list(nullptr),
        where_clause(nullptr),
        group_by(nullptr),
        union_select(nullptr),
        order(nullptr),
        limit(nullptr),
        is_for_update(false){};

  virtual ~SelectStatement() {}

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  std::unique_ptr<TableRef> from_table;
  bool select_distinct;
  std::unique_ptr<std::vector<std::unique_ptr<expression::AbstractExpression>>> select_list;
  std::unique_ptr<expression::AbstractExpression> where_clause;
  std::unique_ptr<GroupByDescription> group_by;

  std::unique_ptr<SelectStatement> union_select;
  std::unique_ptr<OrderDescription> order;
  std::unique_ptr<LimitDescription> limit;
  bool is_for_update;

 public:
  const std::vector<std::unique_ptr<expression::AbstractExpression>>* getSelectList() const {
    return select_list.get();
  }

  void UpdateWhereClause(expression::AbstractExpression* expr) {
    where_clause.reset(expr);
  }
};

}  // End parser namespace
}  // End peloton namespace
