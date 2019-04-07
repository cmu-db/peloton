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

// Curiously recurring template pattern
// TODO(ncx): this templating would be nice to clean up
template <typename T>
struct OperatorNode : public AbstractNode {
  OperatorNode() {}

  virtual ~OperatorNode() {}

  void Accept(OperatorVisitor *v) const;

  virtual std::string GetName() const { return name_; }

  virtual OpType GetType() const { return type_; }

  bool IsLogical() const;

  bool IsPhysical() const;

  static std::string name_;

  static OpType type_;
};

class Operator : public AbstractNode {
 public:
  Operator();

  Operator(AbstractNode *node);

  void Accept(OperatorVisitor *v) const;

  std::string GetName() const;

  OpType GetType() const;

  bool IsLogical() const;

  bool IsPhysical() const;

  hash_t Hash() const;

  bool operator==(const Operator &r);

  bool IsDefined() const;

  template <typename T>
  const T *As() const {
    if (node && typeid(*node) == typeid(T)) {
      return (const T *)node.get();
    }
    return nullptr;
  }

  static std::string name_;

  static OpType type_;

 private:
  std::shared_ptr<AbstractNode> node;
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
