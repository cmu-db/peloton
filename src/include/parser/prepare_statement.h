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

#include "expression/parameter_value_expression.h"
#include "common/sql_node_visitor.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

#include <algorithm>

namespace peloton {
namespace parser {

/**
 * @class PrepareStatement
 * @brief Represents "PREPARE ins_prep: SELECT * FROM t1 WHERE c1 = ? AND c2 =
 * ?"
 */
class PrepareStatement : public SQLStatement {
 public:
  PrepareStatement() : SQLStatement(StatementType::PREPARE), query(nullptr) {}

  virtual ~PrepareStatement() {}

  /**
   * @param vector of placeholders that the parser found
   *
   * When setting the placeholders we need to make sure that they are in the
   *correct order.
   * To ensure that, during parsing we store the character position use that to
   *sort the list here.
   */
  void setPlaceholders(std::vector<void *> ph) {
    for (void *e : ph) {
      if (e != NULL)
        placeholders.push_back(
            std::unique_ptr<expression::ParameterValueExpression>(
                (expression::ParameterValueExpression *)e));
    }
    // Sort by col-id
    std::sort(placeholders.begin(), placeholders.end(),
              [](const std::unique_ptr<expression::ParameterValueExpression> &i,
                 const std::unique_ptr<expression::ParameterValueExpression> &j)
                  -> bool { return (i->ival_ < j->ival_); });

    // Set the placeholder id on the Expr. This replaces the previously stored
    // column id
    for (uint i = 0; i < placeholders.size(); ++i) placeholders[i]->ival_ = i;
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  std::string name;
  std::unique_ptr<SQLStatementList> query;
  std::vector<std::unique_ptr<expression::ParameterValueExpression>>
      placeholders;
};

}  // namespace parser
}  // namespace peloton
