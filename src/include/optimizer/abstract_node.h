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
//TODO(ncx): dependence on OperatorVisitor
class OperatorVisitor;

struct AbstractNode {
  AbstractNode() {}

  ~AbstractNode() {}

  virtual void Accept(OperatorVisitor *v) const = 0;

  virtual std::string GetName() const = 0;

  // TODO(ncx): problematic dependence on OpType
  virtual OpType GetType() const = 0;

  virtual bool IsLogical() const = 0;

  virtual bool IsPhysical() const = 0;

  virtual hash_t Hash() const {
    OpType t = GetType();
    return HashUtil::Hash(&t);
  }

  virtual bool operator==(const AbstractNode &r) {
    return GetType() == r.GetType();
  }

  virtual bool IsDefined() const { return node != nullptr; }

 private:
  std::shared_ptr<AbstractNode> node; 
};

}  // namespace optimizer
}  // namespace peloton
