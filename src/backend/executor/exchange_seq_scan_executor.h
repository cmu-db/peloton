//
// Created by Rui Wang on 16-4-4.
//

#pragma once

#include "backend/expression/abstract_expression.h"
#include "backend/storage/data_table.h"
#include "backend/executor/abstract_exchange_executor.h"
#include "backend/executor/abstract_scan_executor.h"
#include "backend/executor/abstract_parallel_task_response.h"
#include "backend/common/blocking_queue.h"

namespace peloton {
namespace executor {

class ExchangeSeqScanExecutor : public AbstractExchangeExecutor,
                                public AbstractScanExecutor {
 public:
  ExchangeSeqScanExecutor(const ExchangeSeqScanExecutor &) = delete;

  ExchangeSeqScanExecutor &operator=(const ExchangeSeqScanExecutor &) = delete;

  ExchangeSeqScanExecutor(ExchangeSeqScanExecutor &&) = delete;

  ExchangeSeqScanExecutor &operator=(ExchangeSeqScanExecutor &&) = delete;

  explicit ExchangeSeqScanExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context);

  void ThreadExecute(oid_t assigned_tile_group_offset);

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Keeps track of current tile group id being scanned. */
  oid_t current_tile_group_offset_ = INVALID_OID;

  /** @brief Keeps track of the number of tile groups to scan. */
  oid_t table_tile_group_count_ = INVALID_OID;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;

  /** @brief flag if paralleling done. */
  bool parallelize_done_;
};

}  // executor
}  // peloton