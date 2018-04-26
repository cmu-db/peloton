//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hybrid_scan_executor.h
//
// Identification: src/include/executor/hybrid_scan_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_scan_executor.h"
#include "index/index.h"
#include "planner/hybrid_scan_plan.h"
#include "storage/data_table.h"

#include <set>

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
  std::shared_ptr<index::Index> index_;

  storage::DataTable *table_ = nullptr;

  oid_t indexed_tile_offset_ = INVALID_OID;

  HybridScanType type_ = HybridScanType::INVALID;

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

  bool HybridExecPrimaryIndexLookup();
  bool HybridSeqScanUtil();
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

  std::vector<type::Value> values_;

  std::vector<oid_t> full_column_ids_;

  bool key_ready_ = false;

  std::set<ItemPointer> item_pointers_;

  oid_t block_threshold = 0;

  // The predicate used for scanning the index
  index::IndexScanPredicate index_predicate_;
};

}  // namespace executor
}  // namespace peloton
