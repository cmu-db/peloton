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
#include "optimizer/operator_visitor.h"
#include "optimizer/optimizer.h"

#include <cassert>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Base Binding Iterator
//===--------------------------------------------------------------------===//
BindingIterator::BindingIterator(Optimizer &optimizer)
    : optimizer_(optimizer), memo_(optimizer.memo_) {}

//===--------------------------------------------------------------------===//
// Group Binding Iterator
//===--------------------------------------------------------------------===//
GroupBindingIterator::GroupBindingIterator(Optimizer &optimizer, GroupID id,
                                           std::shared_ptr<Pattern> pattern)
    : BindingIterator(optimizer),
      group_id_(id),
      pattern_(pattern),
      target_group_(memo_.GetGroupByID(id)),
      num_group_items_(target_group_->GetExpressions().size()),
      current_item_index_(0) {
  LOG_TRACE("Attempting to bind on group %d", id);
  // We'd like to only explore rules which we know will produce a match of our
  // current pattern. However, because our rules don't currently expose the
  // structure of the output they produce after a transformation, we must be
  // conservative and apply all rules
  const std::vector<std::shared_ptr<GroupExpression>> gexprs =
      target_group_->GetExpressions();
  for (size_t i = 0; i < num_group_items_; ++i) {
    optimizer.ExploreExpression(gexprs[i]);
  }
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
      current_iterator_.reset(new ItemBindingIterator(
          optimizer_, target_group_->GetExpressions()[current_item_index_],
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
ItemBindingIterator::ItemBindingIterator(Optimizer &optimizer,
                                         std::shared_ptr<GroupExpression> gexpr,
                                         std::shared_ptr<Pattern> pattern)
    : BindingIterator(optimizer),
      gexpr_(gexpr),
      pattern_(pattern),
      first_(true),
      has_next_(false),
      current_binding_(std::make_shared<OperatorExpression>(gexpr->Op())) {
  LOG_TRACE("Attempting to bind on group %d with expression of type %s",
            gexpr->GetGroupID(), gexpr->Op().name().c_str());
  if (gexpr->Op().type() != pattern->Type()) return;

  const std::vector<GroupID> &child_groups = gexpr->GetChildGroupIDs();
  const std::vector<std::shared_ptr<Pattern>> &child_patterns =
      pattern->Children();

  if (child_groups.size() != child_patterns.size()) return;

  // Find all bindings for children
  children_bindings_.resize(child_groups.size(), {});
  children_bindings_pos_.resize(child_groups.size(), 0);
  for (size_t i = 0; i < child_groups.size(); ++i) {
    // Try to find a match in the given group
    std::vector<std::shared_ptr<OperatorExpression>> &child_bindings =
        children_bindings_[i];
    GroupBindingIterator iterator(optimizer, child_groups[i],
                                  child_patterns[i]);

    // Get all bindings
    while (iterator.HasNext()) {
      child_bindings.push_back(iterator.Next());
    }

    if (child_bindings.size() == 0) return;

    current_binding_->PushChild(child_bindings[0]);
  }

  has_next_ = true;
}

bool ItemBindingIterator::HasNext() {
  LOG_TRACE("HasNext");
  if (has_next_ && first_) {
    first_ = false;
    return true;
  }

  if (has_next_) {
    size_t size = children_bindings_pos_.size();
    size_t i;
    for (i = 0; i < size; ++i) {
      current_binding_->PopChild();

      const std::vector<std::shared_ptr<OperatorExpression>> &child_binding =
          children_bindings_[size - 1 - i];

      size_t new_pos = ++children_bindings_pos_[size - 1 - i];
      if (new_pos < child_binding.size()) {
        break;
      } else {
        children_bindings_pos_[size - 1 - i] = 0;
      }
    }

    if (i == size) {
      // We have explored all combinations of the child bindings
      has_next_ = false;
    } else {
      // Replay to end
      size_t offset = size - 1 - i;
      for (size_t j = 0; j < i + 1; ++j) {
        const std::vector<std::shared_ptr<OperatorExpression>> &child_binding =
            children_bindings_[offset + j];
        std::shared_ptr<OperatorExpression> binding =
            child_binding[children_bindings_pos_[offset + j]];
        current_binding_->PushChild(binding);
      }
    }
  }
  return has_next_;
}

std::shared_ptr<OperatorExpression> ItemBindingIterator::Next() {
  return current_binding_;
}

} /* namespace optimizer */
} /* namespace peloton */
