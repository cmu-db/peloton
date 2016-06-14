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

#include "common/logger.h"

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

  if (parse_tree.get() == nullptr)
    return NULL;
  std::shared_ptr<planner::AbstractPlan> plan_tree;

/*  std::unique_ptr<planner::AbstractPlan> child_plan;

  // TODO: Transform the parse tree

  // One to one Mapping
  auto parse_item_node_type = parse_tree->GetParseNodeType();

  switch(parse_item_node_type){
    case PLAN_NODE_TYPE_DROP:
      child_plan = new planner::DropPlan();
      break;
    case PLAN_NODE_TYPE_SEQSCAN:
      child_plan = new planner::SeqScanPlan();
  }

  if (child_plan != nullptr) {
      if (plan_tree != nullptr)
        plan_tree->AddChild(child_plan);
      else
        plan_tree = child_plan;
    }*/
  return plan_tree;
}

}  // namespace optimizer
}  // namespace peloton
