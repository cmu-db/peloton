//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/include/optimizer/rule.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include "optimizer/pattern.h"
#include "optimizer/optimize_context.h"
#include "optimizer/operator_expression.h"
#include "optimizer/memo.h"

namespace peloton {
namespace optimizer {

class GroupExpression;

#define PHYS_PROMISE 3
#define LOG_PROMISE 1

class Rule {
 public:
  virtual ~Rule(){};

  std::shared_ptr<Pattern> GetMatchPattern() const { return match_pattern; }

  bool IsPhysical() const { return type_ > RuleType::LogicalPhysicalDelimiter; }

  virtual int Promise(GroupExpression* group_expr,
                      OptimizeContext* context) const;

  virtual bool Check(std::shared_ptr<OperatorExpression> expr,
                     Memo* memo) const = 0;

  virtual void Transform(
      std::shared_ptr<OperatorExpression> input,
      std::vector<std::shared_ptr<OperatorExpression>>& transformed) const = 0;

  inline RuleType GetType() { return type_; }


  inline uint32_t GetRuleIdx() { return static_cast<uint32_t>(type_); }

 protected:
  std::shared_ptr<Pattern> match_pattern;
  RuleType type_;
};

struct RuleWithPromise {
  RuleWithPromise(Rule* rule, int promise) : rule(rule), promise(promise) {}

  Rule* rule;
  int promise;

  bool operator<(const RuleWithPromise& r) const {
    return promise < r.promise;
  }
};

class RuleSet {
 public:
  // RuleSet will take the ownership of the rule object
  RuleSet();

  std::vector<std::unique_ptr<Rule>>& GetRules() { return rules_; }

  inline size_t size() { return rules_.size(); }

 private:
  std::vector<std::unique_ptr<Rule>> rules_;
};

}  // namespace optimizer
}  // namespace peloton
