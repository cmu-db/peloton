//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// join_expr_parse.h
//
// Identification: src/include/parser/peloton/join_expr_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/abstract_parse.h"
#include "parser/table_parse.h"

#include "type/types.h"
#include "common/logger.h"

namespace peloton {
namespace parser {

class JoinExprParse : public AbstractParse {
 public:
  JoinExprParse() = delete;
  JoinExprParse(const JoinExprParse &) = delete;
  JoinExprParse &operator=(const JoinExprParse &) = delete;
  JoinExprParse(DropParse &&) = delete;
  JoinExprParse &operator=(JoinExprParse &&) = delete;

  /*
  explicit JoinExprParse(JoinExpr *join_expr_node) {
    LOG_INFO("Converting JoinExpr...");

    // Don't support natural queries yet
    if (join_expr_node->isNatural) {
      LOG_DEBUG("Convert failure: JoinExpr isNatural");
      return;
    }

    // Don't support using yet
    if (join_expr_node->usingClause) {
      LOG_DEBUG("Convert failure: JoinExpr usingClause");
      return;
    }

    // Don't support alias yet
    if (join_expr_node->alias) {
      LOG_DEBUG("Convert failure: JoinExpr alias");
      return;
    }

    join_type_ = TransformJoinType(join_expr_node->jointype);

    left_node_ = TransformJoinNode(join_expr_node->larg);

    // Track all tables from left child
    GetJoinNodeTables(left_node_tables_, left_node_.get());

    right_node_ = TransformJoinNode(join_expr_node->rarg);
    // Track all tables from right child
    GetJoinNodeTables(right_node_tables_, right_node_.get());

    // Get join predicate
    /* TODO: haven't finished rewrite here
    QueryExpression *predicate = ConvertPostgresQuals(join_expr_node->quals);
    if (predicate == nullptr) {
      LOG_DEBUG("Convert failure: JoinExpr predicate == nullptr");
      return;
    }
    LOG_INFO("Converting JoinExpr succeed.");
  }
   */

  /*
  // transform a postgres parsed join_node to our own parse representation
  static std::unique_ptr<AbstractParse> TransformJoinNode(Node *join_node) {
    std::unique_ptr<AbstractParse> abstract_join_node;

    // There should only be one node because we aren't handling old-style joins
    switch (join_node->type) {
      case T_RangeVar: {
        abstract_join_node.reset(new parser::TableParse((RangeVar *)join_node));
        break;
      }
      case T_JoinExpr: {
        abstract_join_node.reset(
            new parser::JoinExprParse((JoinExpr *)join_node));
        break;
      }
      default:
        LOG_ERROR("Unsupported parse node type : %d ", join_node->type);
    }

    if (abstract_join_node == nullptr) {
      LOG_ERROR("Convert failure: JoinExpr left_child == nullptr");
    }
    return abstract_join_node;
  }

  // Transfrom postgres join type to our own join type.
  static PelotonJoinType TransformJoinType(const JoinType type) {
    switch (type) {
      case JOIN_INNER:
        return JoinType::INNER;
      case JOIN_FULL:
        return JoinType::OUTER;
      case JOIN_LEFT:
        return JoinType::LEFT;
      case JOIN_RIGHT:
        return JoinType::RIGHT;
      case JOIN_SEMI:  // IN+Subquery is JOIN_SEMI
        return JoinType::SEMI;
      default:
        return JoinType::INVALID;
    }
  }
  */

  // Get all base relation tables in a join node.
  static void GetJoinNodeTables(std::vector<TableParse *> &tables,
                                AbstractParse *expr) {
    if (expr->GetParseNodeType() == ParseNodeType::TABLE) {
      return tables.push_back(static_cast<TableParse *>(expr));
    } else if (expr->GetParseNodeType() == ParseNodeType::JOIN_EXPR) {
      JoinExprParse *join = static_cast<JoinExprParse *>(expr);

      tables.insert(tables.end(), join->left_node_tables_.begin(),
                    join->left_node_tables_.end());
      tables.insert(tables.end(), join->right_node_tables_.begin(),
                    join->right_node_tables_.end());
    } else {
      assert(false);
    }
    return {};
  }

  inline ParseNodeType GetParseNodeType() const { return ParseNodeType::DROP; }

  const std::string GetInfo() const { return "DropParse"; }

 private:
  JoinType join_type_;

  // left child of the join tree
  std::unique_ptr<AbstractParse> left_node_;

  // right child of the join tree
  std::unique_ptr<AbstractParse> right_node_;

  AbstractExpressionParse *predicate_;

  // List of all base relations in left node
  std::vector<TableParse *> left_node_tables_;

  // List of all base relations in right node
  std::vector<TableParse *> right_node_tables_;
};

}  // namespace parser
}  // namespace peloton
