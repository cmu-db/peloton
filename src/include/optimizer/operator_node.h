//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_node.h
//
// Identification: src/include/optimizer/operator_node.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/abstract_node.h"
#include "optimizer/property_set.h"
#include "util/hash_util.h"

#include <memory>
#include <string>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator Node
//===--------------------------------------------------------------------===//
class OperatorVisitor;

// TODO: should probably use a new AbstractBaseNode interface, not AbstractNode
template <typename T>
struct OperatorNode : public AbstractNode {
  OperatorNode() : AbstractNode(nullptr) {}

  virtual ~OperatorNode() {}

  void Accept(OperatorVisitor *v) const;

  std::string GetName() const { return name_; }

  OpType GetOpType() const { return op_type_; }

  ExpressionType GetExpType() const { return exp_type_; }

  bool IsLogical() const;

  bool IsPhysical() const;

  static std::string name_;

  static OpType op_type_;

  static ExpressionType exp_type_;
};

class Operator : public AbstractNode {
 public:
  Operator();

  Operator(std::shared_ptr<AbstractNode> node);

  void Accept(OperatorVisitor *v) const;

  std::string GetName() const;

  OpType GetOpType() const;

  ExpressionType GetExpType() const;

  bool IsLogical() const;

  bool IsPhysical() const;

  hash_t Hash() const;

  bool operator==(const Operator &r);

  bool IsDefined() const;
};

}  // namespace optimizer
}  // namespace peloton

namespace std {

template <>
struct hash<peloton::optimizer::AbstractNode> {
  typedef peloton::optimizer::AbstractNode argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
