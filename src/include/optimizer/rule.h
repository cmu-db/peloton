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

#include "optimizer/pattern.h"
#include "optimizer/optimize_context.h"

#include <memory>
#include "operator_expression.h"
#include "memo.h"

namespace peloton {
namespace optimizer {

#define PHYS_PROMISE 3
#define	LOG_PROMISE 1

enum class RuleType {
  INNER_JOIN_COMMUTE,

  LogicalPhysicalDelimiter,

  GET_TO_DUMMY_SCAN,
  GET_TO_SEQ_SCAN,
  GET_TO_INDEX_SCAN,
  QUERY_DERIVED_GET_TO_PHYSICAL,
  DELETE_TO_PHYSICAL,
  UPDATE_TO_PHYSICAL,
  INSERT_TO_PHYSICAL,
  INSERT_SELECT_TO_PHYSICAL,
  AGGREGATE_TO_HASH_AGGREGATE,
  AGGREGATE_TO_SORT_AGGREGATE,
  AGGREGATE_TO_PLAIN_AGGREGATE,
  INNER_JOIN_TO_NL_JOIN,
  INNER_JOIN_TO_HASH_JOIN
};

class Rule {
 public:
  virtual ~Rule() {};

  std::shared_ptr<Pattern> GetMatchPattern() const { return match_pattern; }

  bool IsPhysical() { return RuleType > RuleType::LogicalPhysicalDelimiter; }

  virtual int Promise(std::shared_ptr<OperatorExpression> op_expr, OptimizeContext *context) const {
    auto root_type = match_pattern->Type();
    // This rule is not applicable
    if (root_type != OpType::Leaf && root_type != op_expr->Op().type())
      return 0;
    if (IsPhysical()) return PHYS_PROMISE;
    return LOG_PROMISE;
  }

  virtual bool Check(std::shared_ptr<OperatorExpression> expr, Memo *memo) const = 0;

  virtual void Transform(
      std::shared_ptr<OperatorExpression> input,
      std::vector<std::shared_ptr<OperatorExpression>> &transformed) const = 0;

  inline RuleType GetType() { return type_; }

  inline void SetRuleIdx(int index) {rule_idx_ = index;}

  inline int GetRuleIdx() {return rule_idx_;}


 protected:
  std::shared_ptr<Pattern> match_pattern
  RuleType type_;
  int rule_idx_;
};

struct RuleWithPromise {
  Rule* rule;
  int promise;

  bool operator()(const RuleWithPromise& l, const RuleWithPromise& r) const {
    return l.promise < r.promise;
  }
};

class RuleSet {
 public:
  void AddRule(Rule* rule) {
    rule->SetRuleIdx(rules_.size());
    rules_.emplace_back(rule);
  }

  std::vector<std::unique_ptr<Rule>>& GetRules() {return rules_;}

  inline size_t size() {return rules_.size();}

 private:
  std::vector<std::unique_ptr<Rule>> rules_;
};

} // namespace optimizer
} // namespace peloton
