//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// memo.h
//
// Identification: src/include/optimizer/memo.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <unordered_set>
#include <vector>

#include "operator_expression.h"
#include "optimizer/group.h"

namespace peloton {
namespace optimizer {

template <class Node, class OperatorType, class OperatorExpr>
struct GExprPtrHash {
  std::size_t operator()(GroupExpression<Node,OperatorType,OperatorExpr>* const& s) const { return s->Hash(); }
};

template <class Node, class OperatorType, class OperatorExpr>
struct GExprPtrEq {
  bool operator()(GroupExpression<Node,OperatorType,OperatorExpr>* const& t1,
                  GroupExpression<Node,OperatorType,OperatorExpr>* const& t2) const {
    return *t1 == *t2;
  }
};

//===--------------------------------------------------------------------===//
// Memo
//===--------------------------------------------------------------------===//
template <class Node, class OperatorType, class OperatorExpr>
class Memo {
 public:
  Memo();

  /* InsertExpression - adds a group expression into the proper group in the
   * memo, checking for duplicates
   *
   * expr: the new expression to add
   * enforced: if the new expression is created by enforcer
   * target_group: an optional target group to insert expression into
   * return: existing expression if found. Otherwise, return the new expr
   */
  GroupExpression<Node,OperatorType,OperatorExpr>* InsertExpression(
    std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> gexpr,
    bool enforced);

  GroupExpression<Node,OperatorType,OperatorExpr>* InsertExpression(
    std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> gexpr,
    GroupID target_group, bool enforced);

  std::vector<std::unique_ptr<Group<Node,OperatorType,OperatorExpr>>>& Groups();

  Group<Node,OperatorType,OperatorExpr>* GetGroupByID(GroupID id);

  const std::string GetInfo(int num_indent) const;
  const std::string GetInfo() const;

  inline void SetRuleSetSize(size_t rule_set_size) {
    rule_set_size_ = rule_set_size;
  }

  //===--------------------------------------------------------------------===//
  // For rewrite phase: remove and add expression directly for the set
  //===--------------------------------------------------------------------===//
  void RemoveParExpressionForRewirte(GroupExpression<Node,OperatorType,OperatorExpr>* gexpr) {
    group_expressions_.erase(gexpr);
  }
  void AddParExpressionForRewrite(GroupExpression<Node,OperatorType,OperatorExpr>* gexpr) {
    group_expressions_.insert(gexpr);
  }
  // When a rewrite rule is applied, we need to replace the original gexpr with
  // a new one, which reqires us to first remove the original gexpr from the
  // memo
  void EraseExpression(GroupID group_id) {
    auto gexpr = groups_[group_id]->GetLogicalExpression();
    group_expressions_.erase(gexpr);
    groups_[group_id]->EraseLogicalExpression();
  }

 private:
  GroupID AddNewGroup(std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> gexpr);

  // Internal InsertExpression function
  GroupExpression<Node,OperatorType,OperatorExpr>* InsertExpr(
    std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> gexpr,
    GroupID target_group, bool enforced);

  // The group owns the group expressions, not the memo
  std::unordered_set<GroupExpression<Node,OperatorType,OperatorExpr>*,
                     GExprPtrHash<Node,OperatorType,OperatorExpr>,
                     GExprPtrEq<Node,OperatorType,OperatorExpr>> group_expressions_;
  std::vector<std::unique_ptr<Group<Node,OperatorType,OperatorExpr>>> groups_;
  size_t rule_set_size_;
};

}  // namespace optimizer
}  // namespace peloton
