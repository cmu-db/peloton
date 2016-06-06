//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// memo.cpp
//
// Identification: src/optimizer/memo.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/memo.h"

#include "optimizer/operators.h"

#include <cassert>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Memo
//===--------------------------------------------------------------------===//
Memo::Memo() {}

bool Memo::InsertExpression(std::shared_ptr<GroupExpression> gexpr) {
  return InsertExpression(gexpr, UNDEFINED_GROUP);
}

bool Memo::InsertExpression(std::shared_ptr<GroupExpression> gexpr,
                            GroupID target_group) {
  // If leaf, then just return
  if (gexpr->Op().type() == OpType::Leaf) {
    const LeafOperator *leaf = gexpr->Op().as<LeafOperator>();
    assert(target_group == UNDEFINED_GROUP ||
           target_group == leaf->origin_group);
    gexpr->SetGroupID(leaf->origin_group);
    return false;
  }

  // Lookup in hash table
  auto it = group_expressions.find(gexpr.get());

  bool new_expression;
  if (it != group_expressions.end()) {
    new_expression = false;
    assert(target_group == UNDEFINED_GROUP ||
           target_group == (*it)->GetGroupID());
    gexpr->SetGroupID((*it)->GetGroupID());
  } else {
    new_expression = true;
    group_expressions.insert(gexpr.get());
    // New expression, so try to insert into an existing group or
    // create a new group if none specified
    GroupID group_id;
    if (target_group == UNDEFINED_GROUP) {
      group_id = AddNewGroup();
    } else {
      group_id = target_group;
    }
    Group *group = GetGroupByID(group_id);
    group->AddExpression(gexpr);
  }

  return new_expression;
}

const std::vector<Group> &Memo::Groups() const { return groups; }

Group *Memo::GetGroupByID(GroupID id) { return &(groups[id]); }

GroupID Memo::AddNewGroup() {
  GroupID new_group_id = groups.size();
  groups.emplace_back(new_group_id);
  return new_group_id;
}

} /* namespace optimizer */
} /* namespace peloton */
