//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/include/optimizer/optimizer_task.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

namespace peloton {
namespace optimizer {

class OptimizeContext;
class Memo;
class Rule;
struct RuleWithPromise;
class RuleSet;
class Group;
class GroupExpression;
class OptimizerMetadata;
class PropertySet;
enum class RewriteRuleSetName : uint32_t;
using GroupID = int32_t;

enum class OptimizerTaskType {
  OPTIMIZE_GROUP,
  OPTIMIZE_EXPR,
  EXPLORE_GROUP,
  EXPLORE_EXPR,
  APPLY_RULE,
  OPTIMIZE_INPUTS,
  REWRITE_EXPR,
  APPLY_REWIRE_RULE,
  TOP_DOWN_REWRITE,
  BOTTOM_UP_REWRITE
};

/**
 * @brief The base class for tasks in the optimizer
 */
class OptimizerTask {
 public:
  OptimizerTask(std::shared_ptr<OptimizeContext> context,
                OptimizerTaskType type)
      : type_(type), context_(context) {}

  /**
   * @brief Construct valid rules with their promises for a group expression,
   * promises are used to determine the order of applying the rules. We
   * currently use the promise to enforce that physical rules to be applied
   * before logical rules
   *
   * @param group_expr The group expressions to apply rules
   * @param context The current optimize context
   * @param rules The candidate rule set
   * @param valid_rules The valid rules to apply in the current rule set will be
   *  append to valid_rules, with their promises
   */
  static void ConstructValidRules(GroupExpression *group_expr,
                                  OptimizeContext *context,
                                  std::vector<std::unique_ptr<Rule>> &rules,
                                  std::vector<RuleWithPromise> &valid_rules);

  virtual void execute() = 0;

  void PushTask(OptimizerTask *task);

  inline Memo &GetMemo() const;

  inline RuleSet &GetRuleSet() const;

  virtual ~OptimizerTask(){};

 protected:
  OptimizerTaskType type_;
  std::shared_ptr<OptimizeContext> context_;
};

/**
 * @brief Optimize a group given context. Which will 1. Generatate all logically
 *  equivalent operator trees if not already explored 2. Cost all physical
 *  operator trees given the current context
 */
class OptimizeGroup : public OptimizerTask {
 public:
  OptimizeGroup(Group *group, std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_GROUP),
        group_(group) {}
  virtual void execute() override;

 private:
  Group *group_;
};

/**
 * @brief Optimize an expression by constructing all logical and physical
 *  transformation and applying those rules. Note that we sort all rules by
 * their
 *  promises so that a physical transformation rule is applied before a logical
 *  transformation rule
 */
class OptimizeExpression : public OptimizerTask {
 public:
  OptimizeExpression(GroupExpression *group_expr,
                     std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_EXPR),
        group_expr_(group_expr) {}
  virtual void execute() override;

 private:
  GroupExpression *group_expr_;
};

/**
 * @brief Generate all logical transformation rules by applying logical
 * transformation rules to logical operators in the group until saturated
 */
class ExploreGroup : public OptimizerTask {
 public:
  ExploreGroup(Group *group, std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::EXPLORE_GROUP),
        group_(group) {}
  virtual void execute() override;

 private:
  Group *group_;
};

/**
 * @brief Apply logical transformation rules to a group expression, if a new
 * pattern
 * in the same group is found, also apply logical transformation rule for it.
 */
class ExploreExpression : public OptimizerTask {
 public:
  ExploreExpression(GroupExpression *group_expr,
                    std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::EXPLORE_EXPR),
        group_expr_(group_expr) {}
  virtual void execute() override;

 private:
  GroupExpression *group_expr_;
};

/**
 * @brief Apply rule, if the it's a logical transformation rule, we need to
 *  explore (apply logical rules) or optimize (apply logical & physical rules)
 *  to the new group expression based on the explore flag. If the rule is a
 *  physical implementation rule, we directly cost the physical expression
 */
class ApplyRule : public OptimizerTask {
 public:
  ApplyRule(GroupExpression *group_expr, Rule *rule,
            std::shared_ptr<OptimizeContext> context, bool explore = false)
      : OptimizerTask(context, OptimizerTaskType::APPLY_RULE),
        group_expr_(group_expr),
        rule_(rule),
        explore_only(explore) {}
  virtual void execute() override;

 private:
  GroupExpression *group_expr_;
  Rule *rule_;
  bool explore_only;
};

/**
 * @brief Cost a physical expression. Cost the root operator first then get the
 *  lowest cost of each of the child groups. Finally enforce properties to meet
 *  the requirement in the context. We apply pruning by terminating if the
 *  current expression's cost is larger than the upper bound of the current
 *  group
 */
class OptimizeInputs : public OptimizerTask {
 public:
  OptimizeInputs(GroupExpression *group_expr,
                 std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_INPUTS),
        group_expr_(group_expr) {}

  OptimizeInputs(OptimizeInputs *task)
      : OptimizerTask(task->context_, OptimizerTaskType::OPTIMIZE_INPUTS),
        output_input_properties_(std::move(task->output_input_properties_)),
        group_expr_(task->group_expr_),
        cur_total_cost_(task->cur_total_cost_),
        cur_child_idx_(task->cur_child_idx_),
        cur_prop_pair_idx_(task->cur_prop_pair_idx_) {}

  virtual void execute() override;

 private:
  std::vector<std::pair<std::shared_ptr<PropertySet>,
                        std::vector<std::shared_ptr<PropertySet>>>>
      output_input_properties_;
  GroupExpression *group_expr_;
  double cur_total_cost_;
  int cur_child_idx_ = -1;
  int pre_child_idx_ = -1;
  int cur_prop_pair_idx_ = 0;
};

/**
 * @brief Apply top-down rewrite pass, take in a rule set which must fulfill
 * that the lower level rewrite in the operator tree will not enable upper
 * level rewrite. An example is predicate push-down. We only push the predicates
 * from the upper level to the lower level.
 */
class TopDownRewrite : public OptimizerTask {
 public:
  TopDownRewrite(GroupID group_id, std::shared_ptr<OptimizeContext> context,
                 RewriteRuleSetName rule_set_name)
      : OptimizerTask(context, OptimizerTaskType::TOP_DOWN_REWRITE),
        group_id_(group_id),
        rule_set_name_(rule_set_name) {}
  virtual void execute() override;

 private:
  GroupID group_id_;
  RewriteRuleSetName rule_set_name_;
};

/**
 * @brief Apply bottom-up rewrite pass, take in a rule set which must fulfill
 * that the upper level rewrite in the operator tree will not enable lower
 * level rewrite.
 */
class BottomUpRewrite : public OptimizerTask {
 public:
  BottomUpRewrite(GroupID group_id, std::shared_ptr<OptimizeContext> context,
                  RewriteRuleSetName rule_set_name, bool has_optimized_child)
      : OptimizerTask(context, OptimizerTaskType::BOTTOM_UP_REWRITE),
        group_id_(group_id),
        rule_set_name_(rule_set_name),
        has_optimized_child_(has_optimized_child) {}
  virtual void execute() override;

 private:
  GroupID group_id_;
  RewriteRuleSetName rule_set_name_;
  bool has_optimized_child_;
};
}  // namespace optimizer
}  // namespace peloton
