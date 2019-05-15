//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_node.h
//
// Identification: src/include/optimizer/abstract_node.h
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
  LogicalExternalFileGet,
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
  LogicalExportExternalFile,
  // Separate between logical and physical ops
  LogicalPhysicalDelimiter,
  // Physical ops
  DummyScan, /* Dummy Physical Op for SELECT without FROM*/
  SeqScan,
  IndexScan,
  ExternalFileScan,
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
  SortGroupBy,
  ExportExternalFile,
};

//===--------------------------------------------------------------------===//
// Abstract Node
//===--------------------------------------------------------------------===//
class OperatorVisitor;

struct AbstractNode {
  AbstractNode() {}
  AbstractNode(std::shared_ptr<AbstractNode> node) : node(node) {}

  ~AbstractNode() {}

  //TODO: dependence on OperatorVisitor should ideally be abstracted away
  /**
   * Accepts a visitor
   * @param v Visitor
   */
  virtual void Accept(OperatorVisitor *v) const = 0;

  /**
   * @returns Name fo the Node
   */
  virtual std::string GetName() const = 0;

  // TODO: dependencies on OpType and ExpressionType also not ideal
  /**
   * @returns OpType of the Node
   */
  virtual OpType GetOpType() const = 0;

  /**
   * @returns ExpressionType of the Node
   */
  virtual ExpressionType GetExpType() const = 0;

  /**
   * @returns whether node represents a logical operator / expression
   */
  virtual bool IsLogical() const = 0;

  /**
   * @returns whether node represents a physical operator
   */
  virtual bool IsPhysical() const = 0;

  /**
   * Hashes the AbstractNode
   * @returns hash
   */
  virtual hash_t Hash() const {
    OpType t = GetOpType();
    ExpressionType u = GetExpType();
    return t != OpType::Undefined ? HashUtil::Hash(&t) : HashUtil::Hash(&u);
  }

  /**
   * Base definition of whether two AbstractNodes are equal
   * Function simply checks whether OpType/ExpType match
   */
  virtual bool operator==(const AbstractNode &r) {
    return GetOpType() == r.GetOpType() && GetExpType() == r.GetExpType();
  }

  /**
   * @returns whether the contained node is null or not
   */
  virtual bool IsDefined() const { return node != nullptr; }

  template <typename T>
  const T *As() const {
    if (node && typeid(*node) == typeid(T)) {
      return (const T *)node.get();
    }
    return nullptr;
  }

 protected:
  std::shared_ptr<AbstractNode> node; 
};

}  // namespace optimizer
}  // namespace peloton
