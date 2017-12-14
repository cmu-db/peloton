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

  bool IsPhysical() const {
    return type_ > RuleType::LogicalPhysicalDelimiter && type_ < RuleType::RewriteDelimiter;
  }

  bool IsLogical() const { return type_ < RuleType::LogicalPhysicalDelimiter; }

  bool IsRewrite() const { return type_ > RuleType::RewriteDelimiter; }

  virtual int Promise(GroupExpression* group_expr,
                      OptimizeContext* context) const;

  virtual bool Check(std::shared_ptr<OperatorExpression> expr,
                     OptimizeContext* context) const = 0;

  virtual void Transform(
      std::shared_ptr<OperatorExpression> input,
      std::vector<std::shared_ptr<OperatorExpression>>& transformed, OptimizeContext* context) const = 0;

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

  std::vector<std::unique_ptr<Rule>>& GetTransformationRules() { return transformation_rules_; }
  std::vector<std::unique_ptr<Rule>>& GetImplementationRules() { return implementation_rules_; }
  std::vector<std::unique_ptr<Rule>>& GetRewriteRules() { return rewrite_rules_; }

 private:
  std::vector<std::unique_ptr<Rule>> transformation_rules_;
  std::vector<std::unique_ptr<Rule>> implementation_rules_;
  std::vector<std::unique_ptr<Rule>> rewrite_rules_;

};

}  // namespace optimizer
}  // namespace peloton
