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
#define LOG_PROMISE 1

enum class RuleType : uint32_t {
  INNER_JOIN_COMMUTE = 0,

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
  AGGREGATE_TO_PLAIN_AGGREGATE,
  INNER_JOIN_TO_NL_JOIN,
  INNER_JOIN_TO_HASH_JOIN,

  // Place holder to generate compile time
  NUM_RULES_PLUS_ONE
};


class Rule {
 public:
  virtual ~Rule(){};

  std::shared_ptr<Pattern> GetMatchPattern() const { return match_pattern; }

  bool IsPhysical() const { return type_ > RuleType::LogicalPhysicalDelimiter; }

  virtual int Promise(GroupExpression* group_expr,
                      OptimizeContext* context) const {
    (void)context;
    auto root_type = match_pattern->Type();
    // This rule is not applicable
    if (root_type != OpType::Leaf && root_type != group_expr->Op().type())
      return 0;
    if (IsPhysical()) return PHYS_PROMISE;
    return LOG_PROMISE;
  }

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

  bool operator()(const RuleWithPromise& l, const RuleWithPromise& r) const {
    return l.promise < r.promise;
  }
};

class RuleSet {
 public:
  // RuleSet will take the ownership of the rule object
  RuleSet() {
    rules_.emplace_back(new InnerJoinCommutativity());
    rules_.emplace_back(new LogicalDeleteToPhysical());
    rules_.emplace_back(new LogicalUpdateToPhysical());
    rules_.emplace_back(new LogicalInsertToPhysical());
    rules_.emplace_back(new LogicalInsertSelectToPhysical());
    rules_.emplace_back(new LogicalGroupByToHashGroupBy());
    rules_.emplace_back(new LogicalAggregateToPhysical());
    rules_.emplace_back(new GetToDummyScan());
    rules_.emplace_back(new GetToSeqScan());
    rules_.emplace_back(new GetToIndexScan());
    rules_.emplace_back(new LogicalQueryDerivedGetToPhysical());
    rules_.emplace_back(new InnerJoinToInnerNLJoin());
    rules_.emplace_back(new InnerJoinToInnerHashJoin());
  }

  std::vector<std::unique_ptr<Rule>>& GetRules() { return rules_; }

  inline size_t size() { return rules_.size(); }

 private:
  std::vector<std::unique_ptr<Rule>> rules_;
};

}  // namespace optimizer
}  // namespace peloton
