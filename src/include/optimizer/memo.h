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

struct GExprPtrHash {
  std::size_t operator()(std::shared_ptr<GroupExpression> const& s) const {
    return s->Hash();
  }
};

struct GExprPtrEq {
  bool operator()(std::shared_ptr<GroupExpression> const& t1,
                  std::shared_ptr<GroupExpression> const& t2) const {
    return *t1 == *t2;
  }
};

//===--------------------------------------------------------------------===//
// Memo
//===--------------------------------------------------------------------===//
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
  std::shared_ptr<GroupExpression> InsertExpression(
      std::shared_ptr<GroupExpression> gexpr, bool enforced);

  std::shared_ptr<GroupExpression> InsertExpression(
      std::shared_ptr<GroupExpression> gexpr, GroupID target_group,
      bool enforced);

  const std::vector<Group>& Groups() const;

  Group* GetGroupByID(GroupID id);

 private:
  GroupID AddNewGroup(std::shared_ptr<GroupExpression> gexpr);

  std::unordered_set<std::shared_ptr<GroupExpression>, GExprPtrHash, GExprPtrEq>
      group_expressions_;
  std::vector<Group> groups_;
};

} /* namespace optimizer */
} /* namespace peloton */
