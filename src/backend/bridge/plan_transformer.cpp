/*-------------------------------------------------------------------------
 *
 * plan_transformer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/bridge/plan_transformer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/print.h"
#include "nodes/pprint.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "bridge/bridge.h"
#include "executor/executor.h"
#include "parser/parsetree.h"

#include "executor/nodeValuesscan.h"

#include "backend/common/logger.h"
#include "backend/bridge/expr_transformer.h"
#include "backend/bridge/plan_transformer.h"
#include "backend/bridge/tuple_transformer.h"
#include "backend/common/logger.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/storage/data_table.h"
#include "backend/planner/delete_node.h"
#include "backend/planner/insert_node.h"
#include "backend/planner/limit_node.h"
#include "backend/planner/seq_scan_node.h"
#include "backend/planner/index_scan_node.h"
#include "backend/planner/update_node.h"

#include <cstring>

void printPlanStateTree(const PlanState * planstate);

namespace peloton {
namespace bridge {

std::vector<std::pair<oid_t, expression::AbstractExpression*>>
TransformProjInfo(ProjectionInfo* proj_info, oid_t column_count);

std::vector<std::pair<oid_t, expression::AbstractExpression*>>
TransformTargetList(List* target_list, oid_t column_count);

/*
 * @brief transform a expr tree into info that a index scan node needs
 * */
static void BuildScanKey(const ScanKey scan_keys, int num_keys, planner::IndexScanNode::IndexScanDesc &index_scan_desc);

/**
 * @brief Pretty print the plan state tree.
 * @return none.
 */
void PlanTransformer::PrintPlanState(const PlanState *plan_state) {
  printPlanStateTree(plan_state);
}

/**
 * @brief Convert Postgres PlanState (tree) into AbstractPlanNode (tree).
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlanNode *PlanTransformer::TransformPlan(
    const PlanState *plan_state) {

  Plan *plan = plan_state->plan;
  planner::AbstractPlanNode *plan_node;

  switch (nodeTag(plan)) {
    case T_ModifyTable:
      plan_node = PlanTransformer::TransformModifyTable(
          reinterpret_cast<const ModifyTableState *>(plan_state));
      break;
    case T_SeqScan:
      plan_node = PlanTransformer::TransformSeqScan(
          reinterpret_cast<const SeqScanState*>(plan_state));
      break;
    case T_IndexScan:
      plan_node = PlanTransformer::TransformIndexScan(
          reinterpret_cast<const IndexScanState*>(plan_state));
      break;
    case T_IndexOnlyScan:
      plan_node = PlanTransformer::TransformIndexOnlyScan(
          reinterpret_cast<const IndexOnlyScanState*>(plan_state));
      break;
    case T_Result:
      plan_node = PlanTransformer::TransformResult(
          reinterpret_cast<const ResultState*>(plan_state));
      break;
    case T_Limit:
      plan_node = PlanTransformer::TransformLimit(
          reinterpret_cast<const LimitState*>(plan_state));
      break;
    default: {
      plan_node = nullptr;
      LOG_ERROR("Unsupported Postgres Plan Tag: %u Plan : %p", nodeTag(plan),
                plan);
      break;
    }
  }

  return plan_node;
}

/**
 * @brief Recursively destroy the nodes in a plan node tree.
 */
bool PlanTransformer::CleanPlanNodeTree(planner::AbstractPlanNode* root) {
  if (!root)
    return false;

  // Clean all children subtrees
  auto children = root->GetChildren();
  for (auto child : children) {
    auto rv = CleanPlanNodeTree(child);
    assert(rv);
  }

  // Clean the root
  delete root;
  return true;
}

/**
 * @brief Convert ModifyTableState into AbstractPlanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 *
 * Basically, it multiplexes into helper methods based on operation type.
 */
planner::AbstractPlanNode *PlanTransformer::TransformModifyTable(
    const ModifyTableState *mt_plan_state) {

  ModifyTable *plan = (ModifyTable *) mt_plan_state->ps.plan;

  switch (plan->operation) {
    case CMD_INSERT:
      LOG_INFO("CMD_INSERT");
      return PlanTransformer::TransformInsert(mt_plan_state);
      break;

    case CMD_UPDATE:
      LOG_INFO("CMD_UPDATE");
      return PlanTransformer::TransformUpdate(mt_plan_state);
      break;

    case CMD_DELETE:
      LOG_INFO("CMD_DELETE");
      return PlanTransformer::TransformDelete(mt_plan_state);
      break;

    default:
      LOG_ERROR("Unrecognized operation type : %u", plan->operation);
      break;
  }

  return nullptr;
}

/**
 * @brief Convert ModifyTableState Insert case into AbstractPlanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlanNode *PlanTransformer::TransformInsert(
    const ModifyTableState *mt_plan_state) {

  /* Resolve result table */
  ResultRelInfo *result_rel_info = mt_plan_state->resultRelInfo;
  Relation result_relation_desc = result_rel_info->ri_RelationDesc;

  Oid database_oid = GetCurrentDatabaseOid();
  Oid table_oid = result_relation_desc->rd_id;

  /* Get the target table */
  storage::DataTable *target_table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
          .GetTableWithOid(database_oid, table_oid));

  if (target_table == nullptr) {
    LOG_ERROR("Target table is not found : database oid %u table oid %u",
              database_oid, table_oid);
    return nullptr;
  }

  LOG_INFO("Insert into: database oid %u table oid %u", database_oid, table_oid);

  /* Get the tuple schema */
  auto schema = target_table->GetSchema();

  /* Should be only one sub plan which is a Result  */
  assert(mt_plan_state->mt_nplans == 1);
  assert(mt_plan_state->mt_plans != nullptr);

  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  std::vector<storage::Tuple *> tuples;

  /*
   * We eat our child like Zeus's father to avoid
   * creating a child that returns just a tuple.
   * The cost is to make calls to AbstractExpression->Evaluate() here.
   */
  if (nodeTag(sub_planstate->plan) == T_Result) {  // Child is a result node
    LOG_INFO("Child of Insert is Result");
    auto result_ps = reinterpret_cast<ResultState*>(sub_planstate);

    assert(outerPlanState(result_ps) == nullptr); /* We only handle single-constant-tuple for now,
     i.e., ResultState should have no children/sub plans */

    auto tuple = new storage::Tuple(schema, true);
    auto proj_list = TransformTargetList(
        result_ps->ps.ps_ProjInfo->pi_targetlist, schema->GetColumnCount());

    for (auto proj : proj_list) {
      Value value = proj.second->Evaluate(nullptr, nullptr);  // Constant is expected
      tuple->SetValue(proj.first, value);
    }

    LOG_INFO("Tuple (pl) to insert:");
    std::cout << *tuple << std::endl;

    // TODO Who's responsible for freeing the tuple?
    tuples.push_back(tuple);

  } else {
    LOG_ERROR("Unsupported child type of Insert: %u",
              nodeTag(sub_planstate->plan));
  }

  auto plan_node = new planner::InsertNode(target_table, tuples);

  return plan_node;
}

planner::AbstractPlanNode* PlanTransformer::TransformUpdate(
    const ModifyTableState* mt_plan_state) {

  /*
   * NOTE:
   * In Postgres, the new tuple is returned by an underlying Scan node
   * (by means of non-trivial projections),
   * and the Postgres Update (ModifyTable) node merely replace the old tuple with it.
   * In Peloton, we want to shift the responsibility of constructing the
   * new tuple to the Update node.
   * So, we peek and steal the projection info from our child,
   * but leave it to process the WHERE clause.
   */

  /* Should be only one sub plan which is a SeqScan */
  assert(mt_plan_state->mt_nplans == 1);
  assert(mt_plan_state->mt_plans != nullptr);

  /* Resolve result table */
  ResultRelInfo result_rel_info = mt_plan_state->resultRelInfo[0];
  Relation result_relation_desc = result_rel_info.ri_RelationDesc;

  Oid database_oid = GetCurrentDatabaseOid();
  Oid table_oid = result_relation_desc->rd_id;

  /* Get the target table */
  storage::DataTable *target_table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
          .GetTableWithOid(database_oid, table_oid));

  if (target_table == nullptr) {
    LOG_ERROR("Target table is not found : database oid %u table oid %u",
              database_oid, table_oid);
    return nullptr;
  }

  LOG_INFO("Update table : database oid %u table oid %u", database_oid,
           table_oid);

  /* Get the first sub plan state */
  PlanState *sub_planstate = mt_plan_state->mt_plans[0];
  assert(sub_planstate);

  /* Get the tuple schema */
  auto schema = target_table->GetSchema();

  planner::UpdateNode::ColumnExprs update_column_exprs;
  planner::AbstractPlanNode* plan_node = nullptr;

  if (nodeTag(sub_planstate->plan) == T_SeqScan) {  // Sub plan is SeqScan
    LOG_INFO("Child of Update is SeqScan \n");
    // Extract the non-trivial projection info from SeqScan
    // and put it in our update node
    auto seqscan_state = reinterpret_cast<SeqScanState*>(sub_planstate);

//    update_column_exprs = TransformTargetList(
//        seqscan_state->ps.ps_ProjInfo->pi_targetlist, schema->GetColumnCount());
    update_column_exprs = TransformProjInfo(seqscan_state->ps.ps_ProjInfo, schema->GetColumnCount());

    plan_node = new planner::UpdateNode(target_table, update_column_exprs);
    plan_node->AddChild(TransformPlan(sub_planstate));
  } else {
    LOG_ERROR("Unsupported sub plan type of Update : %u \n",
              nodeTag(sub_planstate->plan));
  }

  return plan_node;
}

/**
 * @brief Convert a Postgres ModifyTableState with DELETE operation
 * into a Peloton DeleteNode.
 * @return Pointer to the constructed AbstractPlanNode.
 *
 * Just like Peloton,
 * the delete plan state in Postgres simply deletes tuples
 *  returned by a subplan (mostly Scan).
 * So we don't need to handle predicates locally .
 */
planner::AbstractPlanNode* PlanTransformer::TransformDelete(
    const ModifyTableState* mt_plan_state) {

  // Grab Database ID and Table ID
  assert(mt_plan_state->resultRelInfo);  // Input must come from a subplan
  assert(mt_plan_state->mt_nplans == 1);  // Maybe relax later. I don't know when they can have >1 subplans.

  Oid database_oid = GetCurrentDatabaseOid();
  Oid table_oid = mt_plan_state->resultRelInfo[0].ri_RelationDesc->rd_id;

  /* Grab the target table */
  storage::DataTable *target_table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
          .GetTableWithOid(database_oid, table_oid));

  assert(target_table);
  LOG_INFO("Delete from: database oid %u table oid %u", database_oid, table_oid);

  /* Grab the subplan -> child plan node */
  assert(mt_plan_state->mt_nplans == 1);
  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  bool truncate = false;

  // Create the peloton plan node
  auto plan_node = new planner::DeleteNode(target_table, truncate);

  // Add child plan node(s)
  plan_node->AddChild(TransformPlan(sub_planstate));

  return plan_node;
}

/**
 * @brief Convert a Postgres SeqScanState into a Peloton SeqScanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 *
 * TODO: Can we also scan result from a child operator? (Non-base-table scan?)
 * We can't for now, but Postgres can.
 */
planner::AbstractPlanNode* PlanTransformer::TransformSeqScan(
    const SeqScanState* ss_plan_state) {

  assert(nodeTag(ss_plan_state) == T_SeqScanState);

  // Grab Database ID and Table ID
  assert(ss_plan_state->ss_currentRelation);  // Null if not a base table scan
  Oid database_oid = GetCurrentDatabaseOid();
  Oid table_oid = ss_plan_state->ss_currentRelation->rd_id;

  /* Grab the target table */
  storage::DataTable *target_table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
          .GetTableWithOid(database_oid, table_oid));

  assert(target_table);
  LOG_INFO("SeqScan: database oid %u table oid %u", database_oid, table_oid);

  /*
   * Grab and transform the predicate.
   *
   * TODO:
   * The qualifying predicate should be extracted from:
   * ss_plan_state->ps.qual (null if no predicate)
   * And remember to free it.
   *
   * Let's just use a null predicate for now.
   */
  expression::AbstractExpression* predicate = nullptr;

  if (ss_plan_state->ps.qual) {

    const ExprState* expr_state = reinterpret_cast<ExprState *>(ss_plan_state
        ->ps.qual);
    predicate = ExprTransformer::TransformExpr(expr_state);

//       int i=0;
//    List       *qual = ss_plan_state->ps.qual;
//    ListCell   *l;
//
//    assert(list_length(qual) == 1);
//
//    foreach(l, qual)
//    {
//      ExprState  *clause = (ExprState *) lfirst(l);
//
//      if(i == 0) {  // Let's just get the first predicate now
//        predicate = ExprTransformer::TransformExpr(clause);
//      }
//      i++;
//    }
  }

  if (predicate) {
    LOG_INFO("Predicate :");
    std::cout << predicate->DebugInfo(" ");
  }

  /*
   * Grab and transform the output column Id's.
   *
   * TODO:
   * The output columns should be extracted from:
   * ss_plan_state->ps.ps_ProjInfo  (null if no projection)
   *
   * Let's just select all columns for now
   */
  auto schema = target_table->GetSchema();
  std::vector<oid_t> column_ids(schema->GetColumnCount());
  std::iota(column_ids.begin(), column_ids.end(), 0);
  assert(column_ids.size() > 0);

  /* Construct and return the Peloton plan node */
  auto plan_node = new planner::SeqScanNode(target_table, predicate,
                                            column_ids);
  return plan_node;
}
/**
 * @brief Convert a Postgres IndexScanState into a Peloton IndexScanNode.
 *        able to handle:
 *          1. simple operator with constant comparison value: indexkey op constant)
 *        unable to handle:
 *          2. redundant simple qualifier: WHERE id > 4 and id > 3
 *          3. simple operator with non-constant value
 *          4. row compare expr: (indexkey, indexkey) op (expr, expr)
 *          5. scalar array op expr: indexkey op ANY (array-expression)
 *          6. null test: indexkey IS NULL/IS NOT NULL
 *          7. order by
 *          8. unary op
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlanNode* PlanTransformer::TransformIndexScan(
    const IndexScanState* iss_plan_state) {
  /* info needed to initialize plan node */
  planner::IndexScanNode::IndexScanDesc index_scan_desc;
   /* Resolve target relation */
  Oid table_oid = iss_plan_state->ss.ss_currentRelation->rd_id;
  Oid database_oid = GetCurrentDatabaseOid();
  const IndexScan* iss_plan = reinterpret_cast<IndexScan*>(iss_plan_state->ss.ps
      .plan);

  storage::DataTable *table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
          .GetTableWithOid(database_oid, table_oid));

  assert(table);

  /* Resolve index  */
  index_scan_desc.index = table->GetIndexWithOid(iss_plan->indexid);
  LOG_INFO("Index scan on oid %u, index name: %s", iss_plan->indexid,
           index_scan_desc.index->GetName().c_str());

  /* Resolve index order */
  /* Only support forward scan direction */
  assert(iss_plan->indexorderdir == ForwardScanDirection);

  /* index qualifier and scan keys */
  LOG_INFO("num of scan keys = %d", iss_plan_state->iss_NumScanKeys);
  BuildScanKey(iss_plan_state->iss_ScanKeys, iss_plan_state->iss_NumScanKeys, index_scan_desc);

  /* handle simple cases */
  /* target list */
  //ioss_plan_state->ss.ps.targetlist;
  /* ORDER BY, not support */

  /* Plan qual, not support */
  //ioss_plan_state->ss.ps.qual;
  auto schema = table->GetSchema();
  index_scan_desc.column_ids.resize(schema->GetColumnCount());
  std::iota(index_scan_desc.column_ids.begin(), index_scan_desc.column_ids.end(), 0);

  return new planner::IndexScanNode(table, index_scan_desc);
}
/**
 * @brief Helper function to build index scan descriptor.
 *        This function assumes the qualifiers are all non-trivial.
 *        i.e. There is no case such as WHERE id > 3 and id > 6
 *        This function can only handle simple constant case
 * @param scan_keys an array of scankey struct from Postgres
 * @param num_keys the number of scan keys
 * @param index_scan_desc the index scan node descriptor in Peloton
 * @return Void
 */
static void BuildScanKey(const ScanKey scan_keys, int num_keys, planner::IndexScanNode::IndexScanDesc &index_scan_desc) {
  const catalog::Schema *schema = index_scan_desc.index->GetKeySchema();

  ScanKey scan_key = scan_keys;
  assert(num_keys > 0);

  for (int i = 0; i < num_keys; i++, scan_key++) {
    assert(!(scan_key->sk_flags & SK_ISNULL)); // currently, only support simple case
    assert(!(scan_key->sk_flags & SK_ORDER_BY)); // currently, only support simple case
    assert(!(scan_key->sk_flags & SK_UNARY)); // currently, only support simple case
    assert(!(scan_key->sk_flags & SK_ROW_HEADER)); // currently, only support simple case
    assert(!(scan_key->sk_flags & SK_ROW_MEMBER)); // currently, only support simple case
    assert(!(scan_key->sk_flags & SK_ROW_END)); // currently, only support simple case
    assert(!(scan_key->sk_flags & SK_SEARCHNULL)); // currently, only support simple case
    assert(!(scan_key->sk_flags & SK_SEARCHNOTNULL)); // currently, only support simple case
    Value value = TupleTransformer::GetValue(scan_key->sk_argument, scan_key->sk_subtype);
    switch(scan_key->sk_strategy) {
      case BTLessStrategyNumber:
        LOG_INFO("<");
        index_scan_desc.end_key = new storage::Tuple(schema, true);
        index_scan_desc.end_key->SetValue(0, value);
        break;
      case BTLessEqualStrategyNumber:
        LOG_INFO("<=");
        index_scan_desc.end_key = new storage::Tuple(schema, true);
        index_scan_desc.end_key->SetValue(0, value);
        index_scan_desc.end_inclusive = true;
        break;
      case BTEqualStrategyNumber:
        LOG_INFO("=");
        index_scan_desc.start_key = new storage::Tuple(schema, true);
        index_scan_desc.end_key = new storage::Tuple(schema, true);
        index_scan_desc.start_key->SetValue(0, value);
        index_scan_desc.end_key->SetValue(0, value);
        index_scan_desc.end_inclusive = true;
        index_scan_desc.start_inclusive = true;
        break;
      case BTGreaterEqualStrategyNumber:
        LOG_INFO(">=");
        index_scan_desc.start_key = new storage::Tuple(schema, true);
        index_scan_desc.start_key->SetValue(0, value);
        index_scan_desc.start_inclusive = true;
        break;
      case BTGreaterStrategyNumber:
        LOG_INFO(">");
        index_scan_desc.start_key = new storage::Tuple(schema, true);
        index_scan_desc.start_key->SetValue(0, value);
        break;
      default:
        LOG_ERROR("Invalid strategy num %d", scan_key->sk_strategy);
        break;
    }
  }
}

/**
 * @brief Convert a Postgres IndexOnlyScanState into a Peloton IndexScanNode.
 *        able to handle:
 *          1. simple operator with constant comparison value: indexkey op constant)
 *        unable to handle:
 *          2. redundant simple qualifier: WHERE id > 4 and id > 3
 *          3. simple operator with non-constant value
 *          4. row compare expr: (indexkey, indexkey) op (expr, expr)
 *          5. scalar array op expr: indexkey op ANY (array-expression)
 *          6. null test: indexkey IS NULL/IS NOT NULL
 *          7. order by
 *          8. unary op
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlanNode* PlanTransformer::TransformIndexOnlyScan(
    const IndexOnlyScanState* ioss_plan_state) {
  /* info needed to initialize plan node */
  planner::IndexScanNode::IndexScanDesc index_scan_desc;

  /* Resolve target relation */
  Oid table_oid = ioss_plan_state->ss.ss_currentRelation->rd_id;
  Oid database_oid = GetCurrentDatabaseOid();
  const IndexScan* iss_plan = reinterpret_cast<IndexScan*>(ioss_plan_state->ss.ps
      .plan);

  storage::DataTable *table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
          .GetTableWithOid(database_oid, table_oid));

  assert(table);

  /* Resolve index  */
  index_scan_desc.index = table->GetIndexWithOid(iss_plan->indexid);
  LOG_INFO("Index scan on oid %u, index name: %s", iss_plan->indexid,
           index_scan_desc.index->GetName().c_str());

  /* Resolve index order */
  /* Only support forward scan direction */
  assert(iss_plan->indexorderdir == ForwardScanDirection);

  /* index qualifier and scan keys */
  LOG_INFO("num of scan keys = %d", ioss_plan_state->ioss_NumScanKeys);
  BuildScanKey(ioss_plan_state->ioss_ScanKeys, ioss_plan_state->ioss_NumScanKeys, index_scan_desc);

  /* handle simple cases */
  /* target list */
  //ioss_plan_state->ss.ps.targetlist;
  /* ORDER BY, not support */

  /* Plan qual, not support */
  //ioss_plan_state->ss.ps.qual;
  auto schema = table->GetSchema();
  index_scan_desc.column_ids.resize(schema->GetColumnCount());
  std::iota(index_scan_desc.column_ids.begin(), index_scan_desc.column_ids.end(), 0);
  return new planner::IndexScanNode(table, index_scan_desc);
 }

/**
 * @brief Convert a Postgres ResultState into a Peloton ResultPlanNode
 * @return Pointer to the constructed AbstractPlanNode
 */
planner::AbstractPlanNode *PlanTransformer::TransformResult(
    const ResultState *node) {
//  ProjectionInfo *projInfo = node->ps.ps_ProjInfo;
//  int numSimpleVars = projInfo->pi_numSimpleVars;
//  ExprDoneCond *itemIsDone = projInfo->pi_itemIsDone;
//  ExprContext *econtext = projInfo->pi_exprContext;
//
//  if (node->rs_checkqual) {
//    LOG_INFO("We can not handle constant qualifications now");
//  }
//
//  if (numSimpleVars > 0) {
//    LOG_INFO("We can not handle simple vars now");
//  }
//
//  if (projInfo->pi_targetlist) {
//    ListCell *tl;
//    List *targetList = projInfo->pi_targetlist;
//
//    LOG_INFO("The number of target in list is %d", list_length(targetList));
//
//    foreach(tl, targetList)
//    {
//      GenericExprState *gstate = (GenericExprState *) lfirst(tl);
//      TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;
//      AttrNumber resind = tle->resno - 1;
//      bool isnull;
//      Datum value = ExecEvalExpr(gstate->arg, econtext, &isnull,
//                                 &itemIsDone[resind]);
//      LOG_INFO("Datum : %lu \n", value);
//    }
//
//  }
//  else {
//    LOG_INFO("We can not handle case where targelist is null");
//  }

  return nullptr;
}

/**
 * @brief Convert a Postgres LimitState into a Peloton LimitPlanNode
 *        does not support LIMIT ALL
 *        does not support cases where there is only OFFSET
 * @return Pointer to the constructed AbstractPlanNode
 */
planner::AbstractPlanNode *PlanTransformer::TransformLimit(
    const LimitState *node) {
  ExprContext *econtext = node->ps.ps_ExprContext;
  Datum val;
  bool isNull;
  int64 limit;
  int64 offset;
  bool noLimit;
  bool noOffset;

  /* Resolve limit and offset */
  if (node->limitOffset) {
    val = ExecEvalExprSwitchContext(node->limitOffset, econtext, &isNull,
    NULL);
    /* Interpret NULL offset as no offset */
    if (isNull)
      offset = 0;
    else {
      offset = DatumGetInt64(val);
      if (offset < 0) {
        LOG_ERROR("OFFSET must not be negative, offset = %ld", offset);
      }
      noOffset = false;
    }
  } else {
    /* No OFFSET supplied */
    offset = 0;
  }

  if (node->limitCount) {
    val = ExecEvalExprSwitchContext(node->limitCount, econtext, &isNull,
    NULL);
    /* Interpret NULL count as no limit (LIMIT ALL) */
    if (isNull) {
      limit = 0;
      noLimit = true;
    } else {
      limit = DatumGetInt64(val);
      if (limit < 0) {
        LOG_ERROR("LIMIT must not be negative, limit = %ld", limit);
      }
      noLimit = false;
    }
  } else {
    /* No LIMIT supplied */
    limit = 0;
    noLimit = true;
  }

  /* TODO: does not do pass down bound to child node
   * In Peloton, they are both unsigned. But both of them cannot be negative,
   * The is safe */
  /* TODO: handle no limit and no offset cases, in which the corresponding value is 0 */
  LOG_INFO("Flags :: Limit: %d, Offset: %d", noLimit, noOffset);
  LOG_INFO("Limit: %ld, Offset: %ld", limit, offset);
  auto plan_node = new planner::LimitNode(limit, offset);

  /* Resolve child plan */
  PlanState *subplan_state = outerPlanState(node);
  assert(subplan_state != nullptr);
  plan_node->AddChild(PlanTransformer::TransformPlan(subplan_state));
  return plan_node;
}

/**
 * @brief Transforms a PG ProjInfo into Peloton.
 * It includes both trivial and non-trivial projections.
 * @param proj_info Postgres ProjectionInfo
 *
 * @param column_count Max column column in the output schema.
 *  This is used to help discard junk attributes, as
 *  we don't need them in Peloton.
 * @return A vector of <column_id, expression> pairs.
 */
std::vector<std::pair<oid_t, expression::AbstractExpression*>>
TransformProjInfo(ProjectionInfo* proj_info, oid_t column_count) {
  std::vector<std::pair<oid_t, expression::AbstractExpression*>> proj_list;

  // 1. Extract the non-trivial projections (expression-based in Postgres)
  proj_list = TransformTargetList(proj_info->pi_targetlist, column_count);

  // 2. Extract the trivial projections
  // (simple var reference, such as 'SELECT b, b, a FROM' or 'SET a=b').
  // Postgres treats them in a short cut, but we don't (at least for now).
  if (proj_info->pi_numSimpleVars > 0)
  {
    int    numSimpleVars = proj_info->pi_numSimpleVars;
    auto    slot = proj_info->pi_slot;
    bool    *isnull = slot->tts_isnull;
    int     *varSlotOffsets = proj_info->pi_varSlotOffsets;
    int     *varNumbers = proj_info->pi_varNumbers;

    if (proj_info->pi_directMap) // Sequential direct map
    {
      /* especially simple case where vars go to output in order */
      for (int out_col_id = 0; out_col_id < numSimpleVars && out_col_id < column_count; out_col_id++)
      {
        // Input should be scan tuple
        assert(varSlotOffsets[out_col_id] ==  offsetof(ExprContext, ecxt_scantuple));

        int     varNumber = varNumbers[out_col_id] - 1;

        oid_t   in_col_id = static_cast<oid_t>(varNumber);

        if(!(isnull[out_col_id])){   // Non null, direct map
          oid_t tuple_idx = 0;
          proj_list.emplace_back(out_col_id,
                                 expression::TupleValueFactory(tuple_idx, in_col_id));
        }
        else { // NUll, constant
          Value null = ValueFactory::GetNullValue();
          proj_list.emplace_back(out_col_id,
                                 expression::ConstantValueFactory(null));
        }

        LOG_INFO("Input column : %u , Output column : %u \n", in_col_id, out_col_id);
      }
    }
    else  // Non-sequential direct map
    {
      /* we have to pay attention to varOutputCols[] */
      int      *varOutputCols = proj_info->pi_varOutputCols;

      for (int i = 0; i < numSimpleVars; i++)
      {
        // Input should be scan tuple
        assert(varSlotOffsets[i] ==  offsetof(ExprContext, ecxt_scantuple));

        int     varNumber = varNumbers[i] - 1;
        int     varOutputCol = varOutputCols[i] - 1;
        oid_t   in_col_id = static_cast<oid_t>(varNumber);
        oid_t   out_col_id = static_cast<oid_t>(varOutputCol);

        if(!(isnull[out_col_id])){   // Non null, direct map
          oid_t tuple_idx = 0;
          proj_list.emplace_back(out_col_id,
                                 expression::TupleValueFactory(tuple_idx, in_col_id));
        }
        else { // NUll, constant
          Value null = ValueFactory::GetNullValue();
          proj_list.emplace_back(out_col_id,
                                 expression::ConstantValueFactory(null));
        }

        LOG_INFO("Input column : %u , Output column : %u \n", in_col_id, out_col_id);
      }
    }
  }

  return proj_list;
}

/**
 * @brief Transforms a non-trivial projection target list
 * (ProjectionInfo.pi_targetList) in Postgres
 * to a Peloton one.
 */
std::vector<std::pair<oid_t, expression::AbstractExpression*>>
TransformTargetList(List* target_list, oid_t column_count) {

  std::vector<std::pair<oid_t, expression::AbstractExpression*>> proj_list;
  ListCell *tl;

  foreach(tl, target_list)
  {
    GenericExprState *gstate = (GenericExprState *) lfirst(tl);
    TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;
    AttrNumber resind = tle->resno - 1;

    if (!(resind < column_count))
      continue;  // skip junk attributes

    LOG_INFO("Target list : column id : %u , Top-level (pg) expr tag : %u \n",
             resind, nodeTag(gstate->arg->expr));

    oid_t col_id = static_cast<oid_t>(resind);

    // TODO: Somebody should be responsible for freeing the expression tree.
    auto peloton_expr = ExprTransformer::TransformExpr(gstate->arg);

    proj_list.emplace_back(col_id, peloton_expr);
  }

  return proj_list;
}

}  // namespace bridge
}  // namespace peloton

