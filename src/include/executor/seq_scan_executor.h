//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.h
//
// Identification: src/include/executor/seq_scan_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/seq_scan_plan.h"
#include "executor/abstract_scan_executor.h"

namespace peloton {
namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class SeqScanExecutor : public AbstractScanExecutor {
 public:
  SeqScanExecutor(const SeqScanExecutor &) = delete;
  SeqScanExecutor &operator=(const SeqScanExecutor &) = delete;
  SeqScanExecutor(SeqScanExecutor &&) = delete;
  SeqScanExecutor &operator=(SeqScanExecutor &&) = delete;

  explicit SeqScanExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context);

  void UpdatePredicate(const std::vector<oid_t> &column_ids,
                       const std::vector<type::Value> &values) override;

  void ResetState() override { current_tile_group_offset_ = START_OID; }

 protected:
  bool DInit() override ;

  bool DExecute() override ;

 private:
  //===--------------------------------------------------------------------===//
  // Helper Functions
  //===--------------------------------------------------------------------===//

  expression::AbstractExpression *ColumnsValuesToExpr(
      const std::vector<oid_t> &predicate_column_ids,
      const std::vector<type::Value> &values, size_t idx);

  expression::AbstractExpression *ColumnValueToCmpExpr(
      const oid_t column_id, const type::Value &value);

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

  bool index_done_ = false;

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;

  // TODO make predicate_ a unique_ptr
  // this is a hack that prevents memory leak
  std::unique_ptr<expression::AbstractExpression> new_predicate_ = nullptr;

  // The original predicate, if it's not nullptr
  // we need to combine it with the undated predicate 
  const expression::AbstractExpression *old_predicate_;
};

}  // namespace executor
}  // namespace peloton
