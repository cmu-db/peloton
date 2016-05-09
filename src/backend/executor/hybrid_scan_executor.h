//
// Created by Rui Wang on 16-4-29.
//

#pragma once

#include "backend/storage/data_table.h"
#include "backend/index/index.h"
#include "backend/executor/abstract_scan_executor.h"
#include "backend/planner/hybrid_scan_plan.h"

namespace peloton {
namespace executor {

class HybridScanExecutor : public AbstractScanExecutor {
public:
  HybridScanExecutor(const HybridScanExecutor &) = delete;
  HybridScanExecutor &operator=(const HybridScanExecutor &) = delete;

  explicit HybridScanExecutor(const planner::AbstractPlan *node,
                     ExecutorContext *executor_context);

protected:
  bool DInit();

  bool DExecute();

private:
  index::Index *index_ = nullptr;

  storage::DataTable *table_ = nullptr;

  oid_t indexed_tile_offset_ = INVALID_OID;

  planner::HybridType type_;

//  bool build_index_ = true;

  // Used for Seq Scan
  /** @brief Keeps track of current tile group id being scanned. */
  oid_t current_tile_group_offset_ = INVALID_OID;

  /** @brief Keeps track of the number of tile groups to scan. */
  oid_t table_tile_group_count_ = INVALID_OID;


  inline bool SeqScanUtil();
  inline bool IndexScanUtil();
  //===--------------------------------------------------------------------===//
  // Index Scan
  //===--------------------------------------------------------------------===//
  bool ExecPrimaryIndexLookup();
//  bool ExecSecondaryIndexLookup();

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of index scan. */
  std::vector<LogicalTile *> result_;

  /** @brief Result itr */
  oid_t result_itr_ = INVALID_OID;

  /** @brief Computed the result */
  bool index_done_ = false;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//
  std::vector<oid_t> column_ids_;

  std::vector<oid_t> key_column_ids_;

  std::vector<ExpressionType> expr_types_;

  std::vector<peloton::Value> values_;

  std::vector<expression::AbstractExpression *> runtime_keys_;

  std::vector<oid_t> full_column_ids_;

  bool key_ready_ = false;

  std::unordered_set<ItemPointer, boost::hash<ItemPointer>> item_pointers_;
};


}  // namespace executor
}  // namespace peloton
