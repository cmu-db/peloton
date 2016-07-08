//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_optimizer.cpp
//
// Identification: src/optimizer/simple_optimizer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "optimizer/simple_optimizer.h"

#include "parser/abstract_parse.h"
#include "parser/insert_parse.h"

#include "planner/abstract_plan.h"
#include "planner/drop_plan.h"
#include "planner/insert_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/create_plan.h"
#include "parser/abstract_parse.h"
#include "parser/drop_parse.h"
#include "parser/create_parse.h"

#include "common/logger.h"

#include <memory>

namespace peloton {
namespace planner {
class AbstractPlan;
}
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

  // Base Case
  if (parse_tree.get() == nullptr)
    return plan_tree;

  std::unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  // TODO: Transform the parse tree

  // One to one Mapping
  auto parse_item_node_type = parse_tree->GetParseNodeType();

  switch (parse_item_node_type) {
    case PARSE_NODE_TYPE_DROP: {
      std::unique_ptr<planner::AbstractPlan> child_DropPlan(
          new planner::DropPlan((parser::DropParse*) parse_tree.get()));
      child_plan = std::move(child_DropPlan);
    }
      break;

    case PARSE_NODE_TYPE_CREATE: {
      std::unique_ptr<planner::AbstractPlan> child_CreatePlan(
          new planner::CreatePlan((parser::CreateParse*) parse_tree.get()));
      child_plan = std::move(child_CreatePlan);
    }
      break;

    case PARSE_NODE_TYPE_SCAN: {
      std::unique_ptr<planner::AbstractPlan> child_SeqScanPlan(
          new planner::SeqScanPlan());
      child_plan = std::move(child_SeqScanPlan);
    }

    case PARSE_NODE_TYPE_INSERT: {
      std::unique_ptr<planner::AbstractPlan> child_InsertPlan(
          new planner::InsertPlan((parser::InsertParse*) parse_tree.get()));
      child_plan = std::move(child_InsertPlan);
    }
      break;

    default:
      LOG_INFO("Unsupported Parse Node Type")
      ;
  }

  if (child_plan != nullptr) {
    if (plan_tree != nullptr)
      plan_tree->AddChild(std::move(child_plan));
    else
      plan_tree = std::move(child_plan);
  }

  // Recurse
  /*auto &children = parse_tree->GetChildren();
   for (auto &child : children) {
   std::shared_ptr<planner::AbstractPlan> child_parse = std::move(BuildPlanTree(child));
   child_plan = std::move(child_parse);
   }*/
  return plan_tree;
}

}  // namespace optimizer
}  // namespace peloton
