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
  : optimizer(optimizer), memo(optimizer.memo)
{
}

//===--------------------------------------------------------------------===//
// Group Binding Iterator
//===--------------------------------------------------------------------===//
GroupBindingIterator::GroupBindingIterator(Optimizer &optimizer,
                                           GroupID id,
                                           std::shared_ptr<Pattern> pattern)
  : BindingIterator(optimizer),
    group_id(id),
    pattern(pattern),
    target_group(memo.GetGroupByID(id)),
    num_group_items(target_group->GetExpressions().size()),
    current_item_index(0)
{
  LOG_TRACE("Attempting to bind on group %d", id);
  // We'd like to only explore rules which we know will produce a match of our
  // current pattern. However, because our rules don't currently expose the
  // structure of the output they produce after a transformation, we must be
  // conservative and apply all rules
  const std::vector<std::shared_ptr<GroupExpression>> gexprs =
    target_group->GetExpressions();
  for (size_t i = 0; i < num_group_items; ++i) {
    optimizer.ExploreExpression(gexprs[i]);
  }
}

bool GroupBindingIterator::HasNext() {
  LOG_TRACE("HasNext");
  if (pattern->Type() == OpType::Leaf) {
    return current_item_index == 0;
  }

  if (current_iterator) {
    // Check if still have bindings in current item
    if (!current_iterator->HasNext()) {
      current_iterator.reset(nullptr);
      current_item_index++;
    }
  }

  if (current_iterator == nullptr) {
    // Keep checking item iterators until we find a match
    while (current_item_index < num_group_items) {
      current_iterator.reset(
        new ItemBindingIterator(
          optimizer,
          target_group->GetExpressions()[current_item_index],
          pattern));

      if (current_iterator->HasNext()) {
        break;
      }

      current_iterator.reset(nullptr);
      current_item_index++;
    }
  }

  return current_iterator != nullptr;
}

std::shared_ptr<OpExpression> GroupBindingIterator::Next() {
  if (pattern->Type() == OpType::Leaf) {
    current_item_index = num_group_items;
    return std::make_shared<OpExpression>(LeafOperator::make(group_id));
  }
  return current_iterator->Next();
}

//===--------------------------------------------------------------------===//
// Item Binding Iterator
//===--------------------------------------------------------------------===//
ItemBindingIterator::ItemBindingIterator(Optimizer &optimizer,
                                         std::shared_ptr<GroupExpression> gexpr,
                                         std::shared_ptr<Pattern> pattern)
  : BindingIterator(optimizer),
    gexpr(gexpr),
    pattern(pattern),
    first(true),
    has_next(false),
    current_binding(std::make_shared<OpExpression>(gexpr->Op()))
{
  LOG_TRACE("Attempting to bind on group %d with expression of type %s",
            gexpr->GetGroupID(), gexpr->Op().name().c_str());
  if (gexpr->Op().type() != pattern->Type()) return;

  const std::vector<GroupID> &child_groups = gexpr->GetChildGroupIDs();
  const std::vector<std::shared_ptr<Pattern>> &child_patterns =
    pattern->Children();

  if (child_groups.size() != child_patterns.size()) return;

  // Find all bindings for children
  children_bindings.resize(child_groups.size(), {});
  children_bindings_pos.resize(child_groups.size(), 0);
  for (size_t i = 0; i < child_groups.size(); ++i) {
    // Try to find a match in the given group
    std::vector<std::shared_ptr<OpExpression>>& child_bindings =
      children_bindings[i];
    GroupBindingIterator iterator(optimizer,
                                  child_groups[i],
                                  child_patterns[i]);

    // Get all bindings
    while (iterator.HasNext()) {
      child_bindings.push_back(iterator.Next());
    }

    if (child_bindings.size() == 0) return;

    current_binding->PushChild(child_bindings[0]);
  }

  has_next = true;
}

bool ItemBindingIterator::HasNext() {
  LOG_TRACE("HasNext");
  if (has_next && first) {
    first = false;
    return true;
  }

  if (has_next) {
    size_t size = children_bindings_pos.size();
    size_t i;
    for (i = 0; i < size; ++i) {
      current_binding->PopChild();

      const std::vector<std::shared_ptr<OpExpression>>& child_binding =
        children_bindings[size - 1 - i];

      size_t new_pos = ++children_bindings_pos[size - 1 - i];
      if (new_pos < child_binding.size()) {
        break;
      } else {
        children_bindings_pos[size - 1 - i] = 0;
      }
    }

    if (i == size) {
      // We have explored all combinations of the child bindings
      has_next = false;
    } else {
      // Replay to end
      size_t offset = size - 1 - i;
      for (size_t j = 0; j < i + 1; ++j) {
        const std::vector<std::shared_ptr<OpExpression>>& child_binding =
          children_bindings[offset + j];
        std::shared_ptr<OpExpression> binding =
          child_binding[children_bindings_pos[offset + j]];
        current_binding->PushChild(binding);
      }
    }
  }
  return has_next;
}

std::shared_ptr<OpExpression> ItemBindingIterator::Next() {
  return current_binding;
}

} /* namespace optimizer */
} /* namespace peloton */
