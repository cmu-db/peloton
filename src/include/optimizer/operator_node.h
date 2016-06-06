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

#include "optimizer/property_set.h"
#include "optimizer/util.h"

#include <string>
#include <memory>

namespace peloton {
namespace optimizer {

enum class OpType {
  Undefined,
  // Special match operators
  Leaf,
  Tree,
  // Logical ops
  Get,
  Project,
  Select,
  InnerJoin,
  LeftJoin,
  RightJoin,
  OuterJoin,
  Aggregate,
  Limit,
  // Physical ops
  Scan,
  ComputeExprs,
  Filter,
  InnerNLJoin,
  LeftNLJoin,
  RightNLJoin,
  OuterNLJoin,
  InnerHashJoin,
  LeftHashJoin,
  RightHashJoin,
  OuterHashJoin,
  // Exprs
  Variable,
  Constant,
  Compare,
  BoolOp,
  Op,
  ProjectList,
  ProjectColumn,
};

//===--------------------------------------------------------------------===//
// Operator Node
//===--------------------------------------------------------------------===//
class OperatorVisitor;

struct BaseOperatorNode {
  BaseOperatorNode() {}

  virtual ~BaseOperatorNode() {}

  virtual void accept(OperatorVisitor *v) const = 0;

  virtual std::string name() const = 0;

  virtual OpType type() const = 0;

  virtual bool IsLogical() const = 0;

  virtual bool IsPhysical() const = 0;

  virtual std::vector<PropertySet> RequiredInputProperties() const {
    return {};
  }

  virtual PropertySet ProvidedOutputProperties() const { return PropertySet(); }

  virtual hash_t Hash() const {
    OpType t = type();
    return util::Hash(&t);
  }

  virtual bool operator==(const BaseOperatorNode &r) {
    return type() == r.type();
  }
};

// Curiously recurring template pattern
template <typename T>
struct OperatorNode : public BaseOperatorNode {
  void accept(OperatorVisitor *v) const;

  std::string name() const { return _name; }

  OpType type() const { return _type; }

  bool IsLogical() const;

  bool IsPhysical() const;

  static std::string _name;

  static OpType _type;
};

class Operator {
 public:
  Operator();

  Operator(BaseOperatorNode *node);

  void accept(OperatorVisitor *v) const;

  std::string name() const;

  OpType type() const;

  bool IsLogical() const;

  bool IsPhysical() const;

  std::vector<PropertySet> RequiredInputProperties() const;

  PropertySet ProvidedOutputProperties() const;

  hash_t Hash() const;

  bool operator==(const Operator &r);

  bool defined() const;

  template <typename T>
  const T *as() const {
    if (node && typeid(*node) == typeid(T)) {
      return (const T *)node.get();
    }
    return nullptr;
  }

 private:
  std::shared_ptr<BaseOperatorNode> node;
};

} /* namespace optimizer */
} /* namespace peloton */

namespace std {

template <>
struct hash<peloton::optimizer::BaseOperatorNode> {
  typedef peloton::optimizer::BaseOperatorNode argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

} /* namespace std */
