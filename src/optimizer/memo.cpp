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

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Memo
//===--------------------------------------------------------------------===//
Memo::Memo() {}

std::shared_ptr<GroupExpression> Memo::InsertExpression(
    std::shared_ptr<GroupExpression> gexpr, bool enforced) {
  return InsertExpression(gexpr, UNDEFINED_GROUP, enforced);
}

std::shared_ptr<GroupExpression> Memo::InsertExpression(
    std::shared_ptr<GroupExpression> gexpr, GroupID target_group,
    bool enforced) {
  // If leaf, then just return
  if (gexpr->Op().type() == OpType::Leaf) {
    const LeafOperator *leaf = gexpr->Op().As<LeafOperator>();
    assert(target_group == UNDEFINED_GROUP ||
           target_group == leaf->origin_group);
    gexpr->SetGroupID(leaf->origin_group);
    return nullptr;
  }

  // Lookup in hash table
  auto it = group_expressions_.find(gexpr);

  if (it != group_expressions_.end()) {
    assert(target_group == UNDEFINED_GROUP ||
           target_group == (*it)->GetGroupID());
    gexpr->SetGroupID((*it)->GetGroupID());
    return *it;
  } else {
    group_expressions_.insert(gexpr);
    // New expression, so try to insert into an existing group or
    // create a new group if none specified
    GroupID group_id;
    if (target_group == UNDEFINED_GROUP) {
      group_id = AddNewGroup(gexpr);
    } else {
      group_id = target_group;
    }
    Group *group = GetGroupByID(group_id);
    group->AddExpression(gexpr, enforced);
    return gexpr;
  }
}

const std::vector<Group> &Memo::Groups() const { return groups_; }

Group *Memo::GetGroupByID(GroupID id) { return &(groups_[id]); }

GroupID Memo::AddNewGroup(std::shared_ptr<GroupExpression> gexpr) {
  GroupID new_group_id = groups_.size();
  // Find out the table alias that this group represents
  std::unordered_set<std::string> table_aliases;
  if (gexpr->Op().type() == OpType::Get) {
    // For base group, the table alias can get directly from logical get
    const LogicalGet *logical_get = gexpr->Op().As<LogicalGet>();
    table_aliases.insert(logical_get->table_alias);
  } else {
    // For other groups, need to aggregate the table alias from children
    for (auto child_group_id : gexpr->GetChildGroupIDs()) {
      Group *child_group = GetGroupByID(child_group_id);
      for (auto &table_alias : child_group->GetTableAliases()) {
        table_aliases.insert(table_alias);
      }
    }
  }
  groups_.emplace_back(new_group_id, std::move(table_aliases));
  return new_group_id;
}

} /* namespace optimizer */
} /* namespace peloton */
