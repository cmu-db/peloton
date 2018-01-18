//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_executor.h
//
// Identification: src/include/executor/order_by_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.h"
#include "executor/abstract_executor.h"
#include "storage/tuple.h"

namespace peloton {
namespace executor {

/**
 * @warning This is a pipeline breaker and a materialization point.
 *
 * TODO Currently, we store all input tiles and sort result in memory
 * until this executor is destroyed, which is sometimes necessary.
 * But can we let it release the RAM earlier as long as the executor
 * is not needed any more (e.g., with a LIMIT sitting on top)?
 *
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class OrderByExecutor : public AbstractExecutor {
 public:
  OrderByExecutor(const OrderByExecutor &) = delete;
  OrderByExecutor &operator=(const OrderByExecutor &) = delete;
  OrderByExecutor(const OrderByExecutor &&) = delete;
  OrderByExecutor &operator=(const OrderByExecutor &&) = delete;

  explicit OrderByExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context);

  ~OrderByExecutor();

 protected:
  bool DInit();

  bool DExecute();

 private:
  bool DoSort();

  bool sort_done_ = false;

  /**
   * IMPORTANT This type must be move-constructible and move-assignable
   * in order to be correctly sorted by STL sort
   */
  struct sort_buffer_entry_t {
    ItemPointer item_pointer;
    std::unique_ptr<storage::Tuple> tuple;

    sort_buffer_entry_t(ItemPointer ipt, std::unique_ptr<storage::Tuple> &&tp)
        : item_pointer(ipt), tuple(std::move(tp)) {}

    sort_buffer_entry_t(sort_buffer_entry_t &&rhs) {
      item_pointer = rhs.item_pointer;
      tuple = std::move(rhs.tuple);
    }

    sort_buffer_entry_t &operator=(sort_buffer_entry_t &&rhs) {
      item_pointer = rhs.item_pointer;
      tuple = std::move(rhs.tuple);
      return *this;
    }

    sort_buffer_entry_t(const sort_buffer_entry_t &) = delete;
    sort_buffer_entry_t &operator=(const sort_buffer_entry_t &) = delete;
  };

  /** All tiles returned by child. */
  std::vector<std::unique_ptr<LogicalTile>> input_tiles_;

  /** Physical (not logical) schema of output tiles */
  std::unique_ptr<catalog::Schema> output_schema_;
  
  /** Projected output column ids corresponding to input schema */
  std::vector<oid_t> output_column_ids_;

  /** All valid tuples in sorted order */
  std::vector<sort_buffer_entry_t> sort_buffer_;

  /** Tuples in sort_buffer only contains the sort keys */
  std::unique_ptr<catalog::Schema> sort_key_tuple_schema_;

  std::vector<bool> descend_flags_;

  /** How many tuples have been returned to parent */
  size_t num_tuples_returned_ = 0;

  /** How many tuples have been get from the child */
  size_t num_tuples_get_ = 0;

  // Copied from plan node
  // Used to show that whether the output is has the same ordering with order by
  // expression. If the so, we can directly used the output result without any
  // additional sorting operation
  bool underling_ordered_ = false;

  // Whether there is limit clause;
  bool limit_ = false;

  // Copied from plan node
  uint64_t limit_number_ = 0;

  // Copied from plan node
  uint64_t limit_offset_ = 0;
};

}  // namespace executor
}  // namespace peloton
