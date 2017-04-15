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

  virtual ~OrderDescription() {
    delete types;

    for (auto expr : *exprs)
      delete expr;

    delete exprs;
  }

  void Accept(SqlNodeVisitor* v) const { v->Visit(this); }

  std::vector<OrderType>* types;
  std::vector<expression::AbstractExpression*>* exprs;
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
  GroupByDescription() : columns(NULL), having(NULL) {}

  ~GroupByDescription() {
    if (columns != nullptr) {
      for (auto col : *columns) delete col;
      delete columns;
    }
    if (having != nullptr)
      delete having;
  }

  void Accept(SqlNodeVisitor* v) const { v->Visit(this); }

  std::vector<expression::AbstractExpression*>* columns;
  expression::AbstractExpression* having;
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
        from_table(NULL),
        select_distinct(false),
        select_list(NULL),
        where_clause(NULL),
        group_by(NULL),
        union_select(NULL),
        order(NULL),
        limit(NULL),
        is_for_update(false){};

  virtual ~SelectStatement() {
    delete from_table;

    if (select_list) {
      for (auto expr : *select_list) delete expr;
      delete select_list;
    }

    delete where_clause;
    delete group_by;

    delete union_select;
    delete order;
    delete limit;
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  TableRef* from_table;
  bool select_distinct;
  std::vector<expression::AbstractExpression*>* select_list;
  expression::AbstractExpression* where_clause;
  GroupByDescription* group_by;

  SelectStatement* union_select;
  OrderDescription* order;
  LimitDescription* limit;
  bool is_for_update;

 public:
  const std::vector<expression::AbstractExpression*>* getSelectList() const {
    return select_list;
  }
};

}  // End parser namespace
}  // End peloton namespace
