//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binding.h
//
// Identification: src/include/optimizer/binding.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_node.h"
#include "optimizer/group.h"
#include "optimizer/pattern.h"
#include <map>
#include <tuple>
#include <memory>
#include "operator_expression.h"

namespace peloton {
namespace optimizer {

class Optimizer;
class Memo;

//===--------------------------------------------------------------------===//
// Binding Iterator
//===--------------------------------------------------------------------===//
class BindingIterator {
 public:
  BindingIterator(Memo& memo) : memo_(memo) {}

  virtual ~BindingIterator(){};

  virtual bool HasNext() = 0;

  virtual std::shared_ptr<OperatorExpression> Next() = 0;

 protected:
  Memo &memo_;
};

class GroupBindingIterator : public BindingIterator {
 public:
  GroupBindingIterator(Memo& memo, GroupID id,
                       std::shared_ptr<Pattern> pattern);

  bool HasNext() override;

  std::shared_ptr<OperatorExpression> Next() override;

 private:
  GroupID group_id_;
  std::shared_ptr<Pattern> pattern_;
  Group *target_group_;
  size_t num_group_items_;

  size_t current_item_index_;
  std::unique_ptr<BindingIterator> current_iterator_;
};

class GroupExprBindingIterator : public BindingIterator {
 public:
  GroupExprBindingIterator(Memo& memo,
                      GroupExpression *gexpr,
                      std::shared_ptr<Pattern> pattern);

  bool HasNext() override;

  std::shared_ptr<OperatorExpression> Next() override;

 private:
  GroupExpression* gexpr_;
  std::shared_ptr<Pattern> pattern_;

  bool first_;
  bool has_next_;
  std::shared_ptr<OperatorExpression> current_binding_;
  std::vector<std::vector<std::shared_ptr<OperatorExpression>>>
      children_bindings_;
  std::vector<size_t> children_bindings_pos_;
};

} // namespace optimizer
} // namespace peloton
