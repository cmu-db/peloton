//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// order_by_executor.h
//
// Identification: src/backend/executor/order_by_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace executor {

/**
 * @warning This is a pipeline breaker and a materialization point.
 *
 * TODO Currently, we store all input tiles and sort result in memory
 * until this executor is destroyed, which is sometimes necessary.
 * But can we let it release the RAM earlier as long as the executor
 * is not needed any more (e.g., with a LIMIT sitting on top)?
 */
class OrderByExecutor : public AbstractExecutor {
 public:
  OrderByExecutor(const OrderByExecutor &) = delete;
  OrderByExecutor &operator=(const OrderByExecutor &) = delete;
  OrderByExecutor(const OrderByExecutor &&) = delete;
  OrderByExecutor &operator=(const OrderByExecutor &&) = delete;

  explicit OrderByExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context);

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

  /** Physical (not logical) schema of input tiles */
  std::unique_ptr<catalog::Schema> input_schema_;

  /** All valid tuples in sorted order */
  std::vector<sort_buffer_entry_t> sort_buffer_;

  /** Tuples in sort_buffer only contains the sort keys */
  std::unique_ptr<catalog::Schema> sort_key_tuple_schema_;

  /** ASC/DESC flags */
  std::vector<bool> descend_flags_;

  /** How many tuples have been returned to parent */
  size_t num_tuples_returned_ = 0;
};

} /* namespace executor */
} /* namespace peloton */
