//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_prepare.h
//
// Identification: src/include/parser/statement_prepare.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "parser/statement_select.h"
#include "expression/abstract_expression.h"

#include <algorithm>

namespace peloton {
namespace parser {

/**
 * @struct PrepareStatement
 * @brief Represents "PREPARE ins_prep: SELECT * FROM t1 WHERE c1 = ? AND c2 =
 * ?"
 */
struct PrepareStatement : SQLStatement {
  PrepareStatement()
      : SQLStatement(STATEMENT_TYPE_PREPARE), name(NULL), query(NULL) {}

  virtual ~PrepareStatement() {
    delete query;
    free(name);

    for (void* e : placeholders) {
      delete (peloton::expression::AbstractExpression*)e;
    }
  }

  /**
   * @param vector of placeholders that the parser found
   *
   * When setting the placeholders we need to make sure that they are in the
   *correct order.
   * To ensure that, during parsing we store the character position use that to
   *sort the list here.
   */
  void setPlaceholders(std::vector<void*> ph) {
    for (void* e : ph) {
      if (e != NULL) placeholders.push_back((expression::AbstractExpression*)e);
    }
    // Sort by col-id
    std::sort(
        placeholders.begin(), placeholders.end(),
        [](expression::AbstractExpression* i, expression::AbstractExpression* j)
            -> bool { return (i->ival < j->ival); });

    // Set the placeholder id on the Expr. This replaces the previously stored
    // column id
    for (uint i = 0; i < placeholders.size(); ++i) placeholders[i]->ival = i;
  }

  char* name;
  SQLStatementList* query;
  std::vector<expression::AbstractExpression*> placeholders;
};

}  // End parser namespace
}  // End peloton namespace
