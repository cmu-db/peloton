//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// select_parse.h
//
// Identification: src/include/parser/peloton/select_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "parser/peloton/abstract_parse.h"
#include "parser/peloton/abstract_expression_parse.h"
#include "parser/peloton/table_parse.h"
#include "parser/peloton/join_expr_parse.h"
#include "parser/peloton/parse_node_visitor.h"

#include "common/types.h"
#include "common/logger.h"

namespace peloton {
namespace parser {

class SelectParse : public AbstractParse {
 public:
  SelectParse() = delete;
  SelectParse(const SelectParse &) = delete;
  SelectParse &operator=(const SelectParse &) = delete;
  SelectParse(SelectParse &&) = delete;
  SelectParse &operator=(SelectParse &&) = delete;

  explicit SelectParse(SelectStmt *select_node) {
    List *from_clause = select_node->fromClause;
    // Cannot handle other case right now
    assert(list_length(from_clause) == 1);

    // Convert join tree
    LOG_INFO("Converting parse jointree");

    join_tree_ = JoinExprParse::TransformJoinNode(
        (Node *)lfirst(list_head(from_clause)));
  }

  inline ParseNodeType GetParseNodeType() const {
    return PARSE_NODE_TYPE_SELECT;
  }

  const std::string GetInfo() const { return "SelectParse"; }
  void accept(ParseNodeVisitor *v) const;

 private:
  // Join tree of the select statement
  // A join node can be either a JoinExprParse or a TableParse.
  std::unique_ptr<AbstractParse> join_tree_;

  // where clause of the select statement
  std::unique_ptr<AbstractExpressionParse> where_predicate_;

  // TODO: support them later!
  // std::vector<std::unique_ptr<Attribute> > output_list_;
  // std::vector<std::unique_ptr<OrderBy> > orderings_;
};

}  // namespace parser
}  // namespace peloton
