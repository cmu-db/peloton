//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// simple_optimizer.cpp
//
// Identification: /peloton/src/optimizer/simple_optimizer.cpp
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/simple_optimizer.h"
#include "parser/peloton/abstract_parse.h"
#include "planner/abstract_plan.h"

#include "common/logger.h"

#include <memory>

namespace peloton {
namespace optimizer {

SimpleOptimizer::SimpleOptimizer() {
}
;

SimpleOptimizer::~SimpleOptimizer() {
}
;

std::shared_ptr<planner::AbstractPlan> SimpleOptimizer::BuildPlanTree(
    const std::unique_ptr<parser::AbstractParse>& parse_tree) {

  std::shared_ptr<planner::AbstractPlan> plan_tree;
  if (parse_tree.get() == nullptr)
     return plan_tree;

 std::unique_ptr<planner::AbstractPlan> child_plan;

  // TODO: Transform the parse tree

  // One to one Mapping
  auto parse_item_node_type = parse_tree->GetParseNodeType();

  switch(parse_item_node_type){
    case PARSE_NODE_TYPE_DROP:
      child_plan = std::make_unique(new planner::DropPlan("department-table"));
      break;

    case PARSE_NODE_TYPE_SCAN:
      child_plan = std::make_unique(new planner::SeqScanPlan());
      break;

    default:
      LOG_INFO("Unsupported Parse Node Type");
  }

// Need to recurse and give base case. for every child in parse tree.

  if (child_plan != nullptr) {
      if (plan_tree != nullptr)
        plan_tree->AddChild(std::move(child_plan));
      else
        plan_tree = child_plan;
    }

  return plan_tree;
}

}  // namespace optimizer
}  // namespace peloton
