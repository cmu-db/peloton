//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// populate_index_executor.h
//
// Identification: src/include/executor/populate_index_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "common/container_tuple.h"
#include "storage/data_table.h"

namespace peloton {
namespace executor {

/**
 * The executor class that populates a newly created index
 *
 * It should have a SeqScanExecutor as a child, for retrieving data
 * to be inputted the index.
 *
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class PopulateIndexExecutor : public AbstractExecutor {
 public:
  PopulateIndexExecutor(const PopulateIndexExecutor &) = delete;
  PopulateIndexExecutor &operator=(const PopulateIndexExecutor &) = delete;
  PopulateIndexExecutor(const PopulateIndexExecutor &&) = delete;
  PopulateIndexExecutor &operator=(const PopulateIndexExecutor &&) = delete;

  explicit PopulateIndexExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context);
  inline storage::DataTable *GetTable() const { return target_table_; }

 protected:
  bool DInit();

  bool DExecute();

 private:
  /** @brief Input tiles from child node */
  std::vector<std::unique_ptr<LogicalTile>> child_tiles_;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;
  std::vector<oid_t> column_ids_;
  bool done_ = false;
};

}  // namespace executor
}  // namespace peloton
