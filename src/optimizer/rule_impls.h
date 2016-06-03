//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_impls.h
//
// Identification: src/backend/optimizer/rule_impls.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/rule.h"

#include <memory>

namespace peloton {
namespace optimizer {

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinCommutativity
class InnerJoinCommutativity : public Rule {
 public:
  InnerJoinCommutativity();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// GetToScan
class GetToScan : public Rule {
 public:
  GetToScan();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// ProjectToComputeExprs
class ProjectToComputeExprs : public Rule {
 public:
  ProjectToComputeExprs();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// SelectToFilter
class SelectToFilter : public Rule {
 public:
  SelectToFilter();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerNLJoin
class InnerJoinToInnerNLJoin : public Rule {
 public:
  InnerJoinToInnerNLJoin();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// LeftJoinToLeftNLJoin
class LeftJoinToLeftNLJoin : public Rule {
 public:
  LeftJoinToLeftNLJoin();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// RightJoinToRightNLJoin
class RightJoinToRightNLJoin : public Rule {
 public:
  RightJoinToRightNLJoin();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// OuterJoinToOuterNLJoin
class OuterJoinToOuterNLJoin : public Rule {
 public:
  OuterJoinToOuterNLJoin();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerHashJoin
class InnerJoinToInnerHashJoin : public Rule {
 public:
  InnerJoinToInnerHashJoin();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// LeftJoinToLeftHashJoin
class LeftJoinToLeftHashJoin : public Rule {
 public:
  LeftJoinToLeftHashJoin();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// RightJoinToRightHashJoin
class RightJoinToRightHashJoin : public Rule {
 public:
  RightJoinToRightHashJoin();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// OuterJoinToOuterHashJoin
class OuterJoinToOuterHashJoin : public Rule {
 public:
  OuterJoinToOuterHashJoin();

  bool Check(std::shared_ptr<OpExpression> plan) const override;

  void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const override;
};

} /* namespace optimizer */
} /* namespace peloton */
