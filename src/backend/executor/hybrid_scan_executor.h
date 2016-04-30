//
// Created by Rui Wang on 16-4-29.
//

#pragma once

#include "backend/storage/data_table.h"
#include "backend/index/index.h"
#include "backend/executor/abstract_scan_executor.h"

namespace peloton {
namespace executor {

enum HybridType {
  UNKNOWN,
  SEQ,
  INDEX,
  HYBRID
};

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
  const index::Index *index_ = nullptr;

  const storage::DataTable *table_ = nullptr;

  oid_t indexed_tile_offset_ = INVALID_OID;

  HybridType type_ = UNKNOWN;


  // Used for Seq Scan
  /** @brief Keeps track of current tile group id being scanned. */
  oid_t current_tile_group_offset_ = INVALID_OID;

  /** @brief Keeps track of the number of tile groups to scan. */
  oid_t table_tile_group_count_ = INVALID_OID;
};


}  // namespace executor
}  // namespace peloton
