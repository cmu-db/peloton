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
  std::size_t operator()(GroupExpression* const& s) const { return s->Hash(); }
};

struct GExprPtrEq {
  bool operator()(GroupExpression* const& t1,
                  GroupExpression* const& t2) const {
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
   * target_group: an optional target group to insert expression into
   * return: true if expression is not a duplicate of an existing expression
   */
  bool InsertExpression(std::shared_ptr<GroupExpression> gexpr);

  bool InsertExpression(std::shared_ptr<GroupExpression> gexpr,
                        GroupID target_group);

  const std::vector<Group>& Groups() const;

  Group* GetGroupByID(GroupID id);

 private:
  GroupID AddNewGroup();

  std::unordered_set<GroupExpression*, GExprPtrHash, GExprPtrEq>
      group_expressions_;
  std::vector<Group> groups_;
};

} /* namespace optimizer */
} /* namespace peloton */
