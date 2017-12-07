//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_impls.h
//
// Identification: src/include/optimizer/rule_impls.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.h"

#include <memory>

namespace peloton {
namespace optimizer {

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinCommutativity
class InnerJoinCommutativity : public Rule {
 public:
  InnerJoinCommutativity();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// GetToScan
class GetToSeqScan : public Rule {
 public:
  GetToSeqScan();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// GetToDummyScan
class GetToDummyScan : public Rule {
 public:
  GetToDummyScan();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// GetToIndexScan
class GetToIndexScan : public Rule {
 public:
  GetToIndexScan();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// LogicalQueryDerivedGetToPhysical
class LogicalQueryDerivedGetToPhysical : public Rule {
 public:
  LogicalQueryDerivedGetToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// LogicalDeleteToPhysical
class LogicalDeleteToPhysical : public Rule {
 public:
  LogicalDeleteToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// LogicalUpdateToPhysical
class LogicalUpdateToPhysical : public Rule {
 public:
  LogicalUpdateToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// LogicalInsertToPhysical
class LogicalInsertToPhysical : public Rule {
 public:
  LogicalInsertToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// LogicalInsertSelectToPhysical
class LogicalInsertSelectToPhysical : public Rule {
 public:
  LogicalInsertSelectToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const
      override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// LogicalGroupByToHashGroupBy
class LogicalGroupByToHashGroupBy : public Rule {
 public:
  LogicalGroupByToHashGroupBy();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};


///////////////////////////////////////////////////////////////////////////////
/// LogicalAggregateToPhysical
class LogicalAggregateToPhysical : public Rule {
 public:
  LogicalAggregateToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerNLJoin
class InnerJoinToInnerNLJoin : public Rule {
 public:
  InnerJoinToInnerNLJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerHashJoin
class InnerJoinToInnerHashJoin : public Rule {
 public:
  InnerJoinToInnerHashJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed)
      const override;
};

} // namespace optimizer
} // namespace peloton
