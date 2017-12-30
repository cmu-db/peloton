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
#include "util/hash_util.h"

#include <memory>
#include <string>

namespace peloton {
namespace optimizer {

enum class OpType {
  Undefined = 0,
  // Special match operators
  Leaf,
  // Logical ops
  Get,
  LogicalQueryDerivedGet,
  LogicalProjection,
  LogicalFilter,
  LogicalMarkJoin,
  LogicalDependentJoin,
  LogicalSingleJoin,
  InnerJoin,
  LeftJoin,
  RightJoin,
  OuterJoin,
  SemiJoin,
  LogicalAggregateAndGroupBy,
  LogicalInsert,
  LogicalInsertSelect,
  LogicalDelete,
  LogicalUpdate,
  LogicalLimit,
  LogicalDistinct,
  // Separate between logical and physical ops
  LogicalPhysicalDelimiter,
  // Physical ops
  DummyScan, /* Dummy Physical Op for SELECT without FROM*/
  SeqScan,
  IndexScan,
  QueryDerivedScan,
  OrderBy,
  PhysicalLimit,
  Distinct,
  InnerNLJoin,
  LeftNLJoin,
  RightNLJoin,
  OuterNLJoin,
  InnerHashJoin,
  LeftHashJoin,
  RightHashJoin,
  OuterHashJoin,
  Insert,
  InsertSelect,
  Delete,
  Update,
  Aggregate,
  HashGroupBy,
  SortGroupBy
};

//===--------------------------------------------------------------------===//
// Operator Node
//===--------------------------------------------------------------------===//
class OperatorVisitor;

struct BaseOperatorNode {
  BaseOperatorNode() {}

  virtual ~BaseOperatorNode() {}

  virtual void Accept(OperatorVisitor *v) const = 0;

  virtual std::string name() const = 0;

  virtual OpType type() const = 0;

  virtual bool IsLogical() const = 0;

  virtual bool IsPhysical() const = 0;

  virtual std::vector<PropertySet> RequiredInputProperties() const {
    return {};
  }

  virtual hash_t Hash() const {
    OpType t = type();
    return HashUtil::Hash(&t);
  }

  virtual bool operator==(const BaseOperatorNode &r) {
    return type() == r.type();
  }
};

// Curiously recurring template pattern
template <typename T>
struct OperatorNode : public BaseOperatorNode {
  void Accept(OperatorVisitor *v) const;

  std::string name() const { return name_; }

  OpType type() const { return type_; }

  bool IsLogical() const;

  bool IsPhysical() const;

  static std::string name_;

  static OpType type_;
};

class Operator {
 public:
  Operator();

  Operator(BaseOperatorNode *node);

  void Accept(OperatorVisitor *v) const;

  std::string name() const;

  OpType type() const;

  bool IsLogical() const;

  bool IsPhysical() const;

  hash_t Hash() const;

  bool operator==(const Operator &r);

  bool defined() const;

  template <typename T>
  const T *As() const {
    if (node && typeid(*node) == typeid(T)) {
      return (const T *)node.get();
    }
    return nullptr;
  }

 private:
  std::shared_ptr<BaseOperatorNode> node;
};

}  // namespace optimizer
}  // namespace peloton

namespace std {

template <>
struct hash<peloton::optimizer::BaseOperatorNode> {
  typedef peloton::optimizer::BaseOperatorNode argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
