//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binding.h
//
// Identification: src/backend/optimizer/binding.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/operator_node.h"
#include "backend/optimizer/group.h"
#include "backend/optimizer/pattern.h"
#include "backend/optimizer/op_expression.h"

#include <map>
#include <tuple>
#include <memory>

namespace peloton {
namespace optimizer {

class Optimizer;
class Memo;

//===--------------------------------------------------------------------===//
// Binding Iterator
//===--------------------------------------------------------------------===//
class BindingIterator {
 public:
  BindingIterator(Optimizer &optimizer);

  virtual ~BindingIterator() {};

  virtual bool HasNext() = 0;

  virtual std::shared_ptr<OpExpression> Next() = 0;

 protected:
  Optimizer &optimizer;
  Memo &memo;
};

class GroupBindingIterator : public BindingIterator {
 public:
  GroupBindingIterator(Optimizer &optimizer,
                       GroupID id,
                       std::shared_ptr<Pattern> pattern);

  bool HasNext() override;

  std::shared_ptr<OpExpression> Next() override;

 private:
  GroupID group_id;
  std::shared_ptr<Pattern> pattern;
  Group *target_group;
  size_t num_group_items;

  size_t current_item_index;
  std::unique_ptr<BindingIterator> current_iterator;
};

class ItemBindingIterator : public BindingIterator {
 public:
  ItemBindingIterator(Optimizer &optimizer,
                      std::shared_ptr<GroupExpression> gexpr,
                      std::shared_ptr<Pattern> pattern);

  bool HasNext() override;

  std::shared_ptr<OpExpression> Next() override;

 private:
  std::shared_ptr<GroupExpression> gexpr;
  std::shared_ptr<Pattern> pattern;

  bool first;
  bool has_next;
  std::shared_ptr<OpExpression> current_binding;
  std::vector<std::vector<std::shared_ptr<OpExpression>>> children_bindings;
  std::vector<size_t> children_bindings_pos;
};

} /* namespace optimizer */
} /* namespace peloton */
