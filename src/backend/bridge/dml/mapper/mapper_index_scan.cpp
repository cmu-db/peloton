/*-------------------------------------------------------------------------
 *
 * mapper_index_scan.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/mapper/mapper_index_scan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/index_scan_node.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Index Scan
//===--------------------------------------------------------------------===//

/*
 * @brief transform a expr tree into info that a index scan node needs
 * */
static void BuildScanKey(
    const ScanKey scan_keys, int num_keys,
    planner::IndexScanNode::IndexScanDesc &index_scan_desc);

/**
 * @brief Convert a Postgres IndexScanState into a Peloton IndexScanNode.
 *        able to handle:
 *          1. simple operator with constant comparison value: indexkey op
 * constant)
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
planner::AbstractPlanNode *PlanTransformer::TransformIndexScan(
    const IndexScanState *iss_plan_state) {
  /* info needed to initialize plan node */
  planner::IndexScanNode::IndexScanDesc index_scan_desc;
  /* Resolve target relation */
  Oid table_oid = iss_plan_state->ss.ss_currentRelation->rd_id;
  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  const IndexScan *iss_plan =
      reinterpret_cast<IndexScan *>(iss_plan_state->ss.ps.plan);

  storage::DataTable *table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

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
  BuildScanKey(iss_plan_state->iss_ScanKeys, iss_plan_state->iss_NumScanKeys,
               index_scan_desc);

  /* handle simple cases */
  /* target list */
  // ioss_plan_state->ss.ps.targetlist;
  /* ORDER BY, not support */

  /* Plan qual, not support */
  // ioss_plan_state->ss.ps.qual;
  auto schema = table->GetSchema();
  index_scan_desc.column_ids.resize(schema->GetColumnCount());
  std::iota(index_scan_desc.column_ids.begin(),
            index_scan_desc.column_ids.end(), 0);

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
static void BuildScanKey(
    const ScanKey scan_keys, int num_keys,
    planner::IndexScanNode::IndexScanDesc &index_scan_desc) {
  const catalog::Schema *schema = index_scan_desc.index->GetKeySchema();

  ScanKey scan_key = scan_keys;
  assert(num_keys > 0);

  for (int i = 0; i < num_keys; i++, scan_key++) {
    assert(!(scan_key->sk_flags &
             SK_ISNULL));  // currently, only support simple case
    assert(!(scan_key->sk_flags &
             SK_ORDER_BY));  // currently, only support simple case
    assert(!(scan_key->sk_flags &
             SK_UNARY));  // currently, only support simple case
    assert(!(scan_key->sk_flags &
             SK_ROW_HEADER));  // currently, only support simple case
    assert(!(scan_key->sk_flags &
             SK_ROW_MEMBER));  // currently, only support simple case
    assert(!(scan_key->sk_flags &
             SK_ROW_END));  // currently, only support simple case
    assert(!(scan_key->sk_flags &
             SK_SEARCHNULL));  // currently, only support simple case
    assert(!(scan_key->sk_flags &
             SK_SEARCHNOTNULL));  // currently, only support simple case
    Value value =
        TupleTransformer::GetValue(scan_key->sk_argument, scan_key->sk_subtype);
    switch (scan_key->sk_strategy) {
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
 *          1. simple operator with constant comparison value: indexkey op
 * constant)
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
planner::AbstractPlanNode *PlanTransformer::TransformIndexOnlyScan(
    const IndexOnlyScanState *ioss_plan_state) {
  /* info needed to initialize plan node */
  planner::IndexScanNode::IndexScanDesc index_scan_desc;

  /* Resolve target relation */
  Oid table_oid = ioss_plan_state->ss.ss_currentRelation->rd_id;
  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  const IndexScan *iss_plan =
      reinterpret_cast<IndexScan *>(ioss_plan_state->ss.ps.plan);

  storage::DataTable *table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

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
  BuildScanKey(ioss_plan_state->ioss_ScanKeys,
               ioss_plan_state->ioss_NumScanKeys, index_scan_desc);

  /* handle simple cases */
  /* target list */
  // ioss_plan_state->ss.ps.targetlist;
  /* ORDER BY, not support */

  /* Plan qual, not support */
  // ioss_plan_state->ss.ps.qual;
  auto schema = table->GetSchema();
  index_scan_desc.column_ids.resize(schema->GetColumnCount());
  std::iota(index_scan_desc.column_ids.begin(),
            index_scan_desc.column_ids.end(), 0);
  return new planner::IndexScanNode(table, index_scan_desc);
}

/**
 * @brief Convert a Postgres BitmapScan into a Peloton IndexScanNode
 *        We currently only handle the case where the lower plan is a
 *BitmapIndexScan
 *
 * @return Pointer to the constructed AbstractPlanNode
 */
planner::AbstractPlanNode *PlanTransformer::TransformBitmapScan(
    const BitmapHeapScanState *bhss_plan_state) {
  planner::IndexScanNode::IndexScanDesc index_scan_desc;

  /* resolve target relation */
  Oid table_oid = bhss_plan_state->ss.ss_currentRelation->rd_id;
  Oid database_oid = Bridge::GetCurrentDatabaseOid();

  assert(nodeTag(outerPlanState(bhss_plan_state)) ==
         T_BitmapIndexScanState);  // only support a bitmap index scan at lower
                                   // level

  const BitmapIndexScanState *biss_state =
      reinterpret_cast<const BitmapIndexScanState *>(
          outerPlanState(bhss_plan_state));
  const BitmapIndexScan *biss_plan =
      reinterpret_cast<const BitmapIndexScan *>(biss_state->ss.ps.plan);

  storage::DataTable *table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  assert(table);
  LOG_INFO("Scan from: database oid %u table oid %u", database_oid, table_oid);

  /* Resolve index  */
  index_scan_desc.index = table->GetIndexWithOid(biss_plan->indexid);
  LOG_INFO("BitmapIdxmap scan on Index oid %u, index name: %s",
           biss_plan->indexid, index_scan_desc.index->GetName().c_str());

  /* Resolve index order */
  /* Only support forward scan direction */

  /* index qualifier and scan keys */
  LOG_INFO("num of scan keys = %d", biss_state->biss_NumScanKeys);
  BuildScanKey(biss_state->biss_ScanKeys, biss_state->biss_NumScanKeys,
               index_scan_desc);

  /* handle simple cases */
  /* target list */
  /* ORDER BY, not support */

  /* Plan qual, not support */

  auto schema = table->GetSchema();
  index_scan_desc.column_ids.resize(schema->GetColumnCount());
  std::iota(index_scan_desc.column_ids.begin(),
            index_scan_desc.column_ids.end(), 0);
  return new planner::IndexScanNode(table, index_scan_desc);
}

}  // namespace bridge
}  // namespace peloton
