//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transitive_predicates.cpp
//
// Identification: src/optimizer/rule_query_rewrite.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "optimizer/rule_query_rewrite.h"
#include "optimizer/operators.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/properties.h"
#include "optimizer/rule_impls.h"
#include "optimizer/util.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Transitive predicates rules
//===--------------------------------------------------------------------===//

///////////////////////////////////////////////////////////////////////////////
/// LogicalGet
TransitivePredicatesLogicalGet::TransitivePredicatesLogicalGet() {
  type_ = RuleType::TRANSITIVE_PREDICATES_LOGICAL_GET;

  match_pattern = std::make_shared<Pattern>(OpType::Get);
}

bool TransitivePredicatesLogicalGet::Check(std::shared_ptr<OperatorExpression> input,
					   OptimizeContext *context) const {
  (void)input;
  (void)context;
  return true;
}

void TransitivePredicatesLogicalGet::Transform(
    std::shared_ptr<OperatorExpression> input,
    UNUSED_ATTRIBUTE std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    OptimizeContext *context) const {
  LOG_TRACE("TransitivePredicatesLogicalGet::Transform");

  auto get = input->Op().As<LogicalGet>();
  auto &get_predicates = get->predicates;

  LOG_DEBUG("---------- Number of predicates %d\n", int(get_predicates.size()));
  util::FillTransitiveTable(get_predicates, context->transitive_table);
  auto new_predicates = util::GenerateTransitivePredicates(get_predicates, context->transitive_table);

  if (new_predicates.empty())
    return;

  std::vector<AnnotatedExpression> total_predicates;

  for (auto &predicate : get_predicates) {
    total_predicates.emplace_back(predicate);
  }

  for (auto &predicate : new_predicates) {
    total_predicates.emplace_back(predicate);
  }
  
  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(
          LogicalGet::make(get->get_id, total_predicates,
  			   get->table, get->table_alias, get->is_for_update));
  
  transformed.push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalFilter
TransitivePredicatesLogicalFilter::TransitivePredicatesLogicalFilter() {
  type_ = RuleType::TRANSITIVE_PREDICATES_LOGICAL_FILTER;

  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::InnerJoin));
  child->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  child->AddChild(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern = std::make_shared<Pattern>(OpType::LogicalFilter);

  match_pattern->AddChild(child);
}

bool TransitivePredicatesLogicalFilter::Check(std::shared_ptr<OperatorExpression> input,
					      OptimizeContext *context) const {
  (void)input;
  (void)context;
  return true;
}

void TransitivePredicatesLogicalFilter::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    OptimizeContext *context) const {
  LOG_TRACE("TransitivePredicatesLogicalFilter::Transform");

  auto filter = input->Op().As<LogicalFilter>();
  auto &filter_predicates = filter->predicates;

  util::FillTransitiveTable(filter_predicates, context->transitive_table);
  auto new_predicates = util::GenerateTransitivePredicates(filter_predicates, context->transitive_table);

  if (new_predicates.empty())
    return;

  std::vector<AnnotatedExpression> total_predicates;

  for (auto &predicate : filter_predicates) {
    total_predicates.emplace_back(predicate);
  }

  for (auto &predicate : new_predicates) {
    total_predicates.emplace_back(predicate);
  }
  
  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(
          LogicalFilter::make(total_predicates));

  auto filter_children = input->Children();
  for (auto child : filter_children) {
    output->PushChild(child);
  }

  transformed.push_back(output);
}

//===--------------------------------------------------------------------===//
// Simplify predicates rules
//===--------------------------------------------------------------------===//

///////////////////////////////////////////////////////////////////////////////
/// LogicalFilter
SimplifyPredicatesLogicalFilter::SimplifyPredicatesLogicalFilter() {
  type_ = RuleType::SIMPLIFY_PREDICATES_LOGICAL_FILTER;

  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::InnerJoin));
  child->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  child->AddChild(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern = std::make_shared<Pattern>(OpType::LogicalFilter);

  match_pattern->AddChild(child);
}

bool SimplifyPredicatesLogicalFilter::Check(std::shared_ptr<OperatorExpression> input,
					    OptimizeContext *context) const {
  (void)input;
  (void)context;
  return true;
}

void SimplifyPredicatesLogicalFilter::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  LOG_TRACE("SimplifyPredicatesLogicalFilter::Transform");

  auto filter = input->Op().As<LogicalFilter>();
  auto &filter_predicates = filter->predicates;

  auto new_predicates = util::SimplifyPredicates(filter_predicates);

  PELOTON_ASSERT(new_predicates.size() <= filter_predicates.size());

  if (new_predicates.size() == filter_predicates.size())
    return;
  
  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(
          LogicalFilter::make(new_predicates));

  auto filter_children = input->Children();
  for (auto &child : filter_children) {
    output->PushChild(child);
  }

  transformed.push_back(output);
}
}  // namespace optimizer
}  // namespace peloton
