//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/dml/executor/plan_executor.h"
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/dml/tuple/tuple_transformer.h"
#include "backend/common/logger.h"
#include "backend/common/cache.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/abstract_plan.h"
#include "backend/planner/project_info.h"
#include "backend/planner/insert_plan.h"

#include "nodes/print.h"
#include "nodes/pprint.h"
#include "utils/lsyscache.h"
#include "parser/parsetree.h"

namespace peloton {
namespace bridge {

const PlanTransformer::TransformOptions PlanTransformer::DefaultOptions;

PlanTransformer &PlanTransformer::GetInstance() {
  thread_local static PlanTransformer transformer;
  return transformer;
}

std::shared_ptr<const planner::AbstractPlan> PlanTransformer::GetCachedPlan(
    const char *prepStmtName) {
  if (!prepStmtName) {
    return nullptr;
  }

  std::string name_str(prepStmtName);

  auto itr = plan_cache_.find(name_str);
  if (itr == plan_cache_.end()) {
    /* A plan cache miss */
    LOG_TRACE("Cache miss for %s", name_str.c_str());
    return nullptr;
  } else {
    /* A plan cache hit */
    LOG_TRACE("Cache hit for %s", name_str.c_str());
    return *itr;
  }
}

std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformPlan(
    AbstractPlanState *planstate) {
  return std::move(TransformPlan(planstate, DefaultOptions));
}

std::shared_ptr<const planner::AbstractPlan> PlanTransformer::TransformPlan(
    AbstractPlanState *planstate, const char *prepStmtName) {
  auto mapped_plan = TransformPlan(planstate, DefaultOptions);
  std::shared_ptr<const planner::AbstractPlan> mapped_plan_ptr(
      std::move(mapped_plan));
  if (prepStmtName) {
    std::string name_str(prepStmtName);
    plan_cache_.insert(std::make_pair(std::move(name_str), mapped_plan_ptr));
  }
  return mapped_plan_ptr;
}

/**
 * @brief Convert Postgres Plan (tree) into AbstractPlan (tree).
 * @return Pointer to the constructed AbstractPlan Node.
 */
std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformPlan(
    AbstractPlanState *planstate, const TransformOptions options) {
  ALWAYS_ASSERT(planstate);

  // Ignore empty plans
  if (planstate == nullptr) return nullptr;

  std::unique_ptr<planner::AbstractPlan> peloton_plan;

  switch (nodeTag(planstate)) {
    case T_ModifyTableState:
      peloton_plan = std::move(PlanTransformer::TransformModifyTable(
              reinterpret_cast<const ModifyTablePlanState *>(planstate),
              options));
      break;
    case T_SeqScanState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformSeqScan(
              reinterpret_cast<const SeqScanPlanState *>(planstate), options)));
      break;
    case T_IndexScanState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformIndexScan(
          reinterpret_cast<const IndexScanPlanState *>(planstate), options)));
      break;
    case T_IndexOnlyScanState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformIndexOnlyScan(
          reinterpret_cast<const IndexOnlyScanPlanState *>(planstate),
          options)));
      break;
    case T_BitmapHeapScanState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformBitmapHeapScan(
          reinterpret_cast<const BitmapHeapScanPlanState *>(planstate),
          options)));
      break;
    case T_LockRowsState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformLockRows(
          reinterpret_cast<const LockRowsPlanState *>(planstate))));
      break;
    case T_LimitState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformLimit(
          reinterpret_cast<const LimitPlanState *>(planstate))));
      break;
    case T_MergeJoinState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformMergeJoin(
          reinterpret_cast<const MergeJoinPlanState *>(planstate))));
      break;
    case T_HashJoinState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformHashJoin(
          reinterpret_cast<const HashJoinPlanState *>(planstate))));
      break;
    case T_NestLoopState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformNestLoop(
          reinterpret_cast<const NestLoopPlanState *>(planstate))));
      break;
    case T_MaterialState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformMaterialization(
          reinterpret_cast<const MaterialPlanState *>(planstate))));
      break;
    case T_AggState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformAgg(
          reinterpret_cast<const AggPlanState *>(planstate))));
      break;

    case T_SortState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformSort(
          reinterpret_cast<const SortPlanState *>(planstate))));
      break;

    case T_HashState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
           PlanTransformer::TransformHash(
          reinterpret_cast<const HashPlanState *>(planstate))));
      break;

    case T_UniqueState:
      peloton_plan = std::unique_ptr<planner::AbstractPlan>(std::move(
          PlanTransformer::TransformUnique(
          reinterpret_cast<const UniquePlanState *>(planstate))));
      break;

    default: {
      LOG_ERROR("PlanTransformer :: Unsupported Postgres Plan Tag: %u ",
                nodeTag(planstate));
      elog(INFO, "Query: ");
      break;
    }
  }

  return peloton_plan;
}

}  // namespace bridge
}  // namespace peloton
