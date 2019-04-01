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

template <class Node, class OperatorType, class OperatorExpr>
class Memo;

//===--------------------------------------------------------------------===//
// Binding Iterator
//===--------------------------------------------------------------------===//
template <class Node, class OperatorType, class OperatorExpr>
class BindingIterator {
 public:
  BindingIterator(Memo<Node,OperatorType,OperatorExpr>& memo) : memo_(memo) {}

  virtual ~BindingIterator(){};

  virtual bool HasNext() = 0;

  virtual std::shared_ptr<OperatorExpr> Next() = 0;

 protected:
  Memo<Node,OperatorType,OperatorExpr> &memo_;
};

template <class Node, class OperatorType, class OperatorExpr>
class GroupBindingIterator : public BindingIterator<Node,OperatorType,OperatorExpr> {
 public:
  GroupBindingIterator(Memo<Node,OperatorType,OperatorExpr>& memo, 
                       GroupID id,
                       std::shared_ptr<Pattern<OperatorType>> pattern);

  bool HasNext() override;

  std::shared_ptr<OperatorExpr> Next() override;

 private:
  GroupID group_id_;
  std::shared_ptr<Pattern<OperatorType>> pattern_;
  Group<Node,OperatorType,OperatorExpr> *target_group_;
  size_t num_group_items_;

  size_t current_item_index_;
  std::unique_ptr<BindingIterator<Node,OperatorType,OperatorExpr>> current_iterator_;
};

template <class Node, class OperatorType, class OperatorExpr>
class GroupExprBindingIterator : public BindingIterator<Node,OperatorType,OperatorExpr> {
 public:
  GroupExprBindingIterator(Memo<Node,OperatorType,OperatorExpr>& memo,
                           GroupExpression<Node,OperatorType,OperatorExpr> *gexpr,
                           std::shared_ptr<Pattern<OperatorType>> pattern);

  bool HasNext() override;

  std::shared_ptr<OperatorExpr> Next() override;

 private:
  GroupExpression<Node,OperatorType,OperatorExpr>* gexpr_;
  std::shared_ptr<Pattern<OperatorType>> pattern_;

  bool first_;
  bool has_next_;
  std::shared_ptr<OperatorExpr> current_binding_;
  std::vector<std::vector<std::shared_ptr<OperatorExpr>>> children_bindings_;
  std::vector<size_t> children_bindings_pos_;
};

} // namespace optimizer
} // namespace peloton
