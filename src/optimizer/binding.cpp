//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binding.cpp
//
// Identification: src/optimizer/binding.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/binding.h"

#include "common/logger.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimizer.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Group Binding Iterator
//===--------------------------------------------------------------------===//
GroupBindingIterator::GroupBindingIterator(Memo &memo, GroupID id,
                                           std::shared_ptr<Pattern> pattern)
    : BindingIterator(memo),
      group_id_(id),
      pattern_(pattern),
      target_group_(memo_.GetGroupByID(id)),
      num_group_items_(target_group_->GetLogicalExpressions().size()),
      current_item_index_(0) {
  LOG_TRACE("Attempting to bind on group %d", id);
}

bool GroupBindingIterator::HasNext() {
  LOG_TRACE("HasNext");
  if (pattern_->Type() == OpType::Leaf) {
    return current_item_index_ == 0;
  }

  if (current_iterator_) {
    // Check if still have bindings in current item
    if (!current_iterator_->HasNext()) {
      current_iterator_.reset(nullptr);
      current_item_index_++;
    }
  }

  if (current_iterator_ == nullptr) {
    // Keep checking item iterators until we find a match
    while (current_item_index_ < num_group_items_) {
      current_iterator_.reset(new GroupExprBindingIterator(
          memo_,
          target_group_->GetLogicalExpressions()[current_item_index_].get(),
          pattern_));

      if (current_iterator_->HasNext()) {
        break;
      }

      current_iterator_.reset(nullptr);
      current_item_index_++;
    }
  }

  return current_iterator_ != nullptr;
}

std::shared_ptr<OperatorExpression> GroupBindingIterator::Next() {
  if (pattern_->Type() == OpType::Leaf) {
    current_item_index_ = num_group_items_;
    return std::make_shared<OperatorExpression>(LeafOperator::make(group_id_));
  }
  return current_iterator_->Next();
}

//===--------------------------------------------------------------------===//
// Item Binding Iterator
//===--------------------------------------------------------------------===//
GroupExprBindingIterator::GroupExprBindingIterator(
    Memo &memo, GroupExpression *gexpr, std::shared_ptr<Pattern> pattern)
    : BindingIterator(memo),
      gexpr_(gexpr),
      pattern_(pattern),
      first_(true),
      has_next_(false),
      current_binding_(std::make_shared<OperatorExpression>(gexpr->Op())) {
  if (gexpr->Op().GetType() != pattern->Type()) {
    return;
  }

  const std::vector<GroupID> &child_groups = gexpr->GetChildGroupIDs();
  const std::vector<std::shared_ptr<Pattern>> &child_patterns =
      pattern->Children();

  if (child_groups.size() != child_patterns.size()) {
    return;
  }
  LOG_TRACE(
      "Attempting to bind on group %d with expression of type %s, children "
      "size %lu",
      gexpr->GetGroupID(), gexpr->Op().GetName().c_str(), child_groups.size());

  // Find all bindings for children
  children_bindings_.resize(child_groups.size(), {});
  children_bindings_pos_.resize(child_groups.size(), 0);
  for (size_t i = 0; i < child_groups.size(); ++i) {
    // Try to find a match in the given group
    std::vector<std::shared_ptr<OperatorExpression>> &child_bindings =
        children_bindings_[i];
    GroupBindingIterator iterator(memo_, child_groups[i], child_patterns[i]);

    // Get all bindings
    while (iterator.HasNext()) {
      child_bindings.push_back(iterator.Next());
    }

    if (child_bindings.size() == 0) {
      return;
    }

    current_binding_->PushChild(child_bindings[0]);
  }

  has_next_ = true;
}

bool GroupExprBindingIterator::HasNext() {
  LOG_TRACE("HasNext");
  if (has_next_ && first_) {
    first_ = false;
    return true;
  }

  if (has_next_) {
    // The first child to be modified
    int first_modified_idx = children_bindings_pos_.size() - 1;
    for (; first_modified_idx >= 0; --first_modified_idx) {
      const std::vector<std::shared_ptr<OperatorExpression>> &child_binding =
          children_bindings_[first_modified_idx];

      // Try to increment idx from the back
      size_t new_pos = ++children_bindings_pos_[first_modified_idx];
      if (new_pos < child_binding.size()) {
        break;
      } else {
        children_bindings_pos_[first_modified_idx] = 0;
      }
    }

    if (first_modified_idx < 0) {
      // We have explored all combinations of the child bindings
      has_next_ = false;
    } else {
      // Pop all updated childrens
      for (size_t idx = first_modified_idx; idx < children_bindings_pos_.size();
           idx++) {
        current_binding_->PopChild();
      }
      // Add new children to end
      for (size_t offset = first_modified_idx;
           offset < children_bindings_pos_.size(); ++offset) {
        const std::vector<std::shared_ptr<OperatorExpression>> &child_binding =
            children_bindings_[offset];
        std::shared_ptr<OperatorExpression> binding =
            child_binding[children_bindings_pos_[offset]];
        current_binding_->PushChild(binding);
      }
    }
  }
  return has_next_;
}

std::shared_ptr<OperatorExpression> GroupExprBindingIterator::Next() {
  return current_binding_;
}

}  // namespace optimizer
}  // namespace peloton
