//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_executor.cpp
//
// Identification: src/executor/order_by_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>

#include "common/logger.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/order_by_executor.h"
#include "executor/executor_context.h"

#include "planner/order_by_plan.h"
#include "storage/tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 * @param node  OrderByNode plan node corresponding to this executor
 */
OrderByExecutor::OrderByExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

OrderByExecutor::~OrderByExecutor() {}

bool OrderByExecutor::DInit() {
  PL_ASSERT(children_.size() == 1);

  sort_done_ = false;
  num_tuples_returned_ = 0;

  // Grab info from plan node and check it
  const planner::OrderByPlan &node = GetPlanNode<planner::OrderByPlan>();

  // copy from underlying plan
  underling_ordered_ = node.GetUnderlyingOrder();

  // Whether there is limit clause
  limit_ = node.GetLimit();

  // Copied from plan node
  limit_number_ = node.GetLimitNumber();

  // Copied from plan node
  limit_offset_ = node.GetLimitOffset();

  return true;
}

bool OrderByExecutor::DExecute() {
  LOG_TRACE("Order By executor ");

  if (!sort_done_) DoSort();

  if (!(num_tuples_returned_ < sort_buffer_.size())) {
    return false;
  }

  PL_ASSERT(sort_done_);
  PL_ASSERT(output_schema_.get());
  PL_ASSERT(input_tiles_.size() > 0);

  // Returned tiles must be newly created physical tiles.
  // NOTE: the schema of these tiles might not match the input tiles when some of the order by columns are not be part of the output schema

  size_t tile_size = std::min(size_t(DEFAULT_TUPLES_PER_TILEGROUP),
                              sort_buffer_.size() - num_tuples_returned_);

  std::shared_ptr<storage::Tile> ptile(storage::TileFactory::GetTile(
      BackendType::MM, INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
      nullptr, *output_schema_, nullptr, tile_size));

  for (size_t id = 0; id < tile_size; id++) {
    oid_t source_tile_id =
        sort_buffer_[num_tuples_returned_ + id].item_pointer.block;
    oid_t source_tuple_id =
        sort_buffer_[num_tuples_returned_ + id].item_pointer.offset;
    // Insert a physical tuple into physical tile
    for (oid_t i = 0; i < output_schema_->GetColumnCount(); i++) {
      type::Value val =
          (input_tiles_[source_tile_id]->GetValue(source_tuple_id, output_column_ids_[i]));
      ptile.get()->SetValue(val, id, i);
    }
  }

  // Create an owner wrapper of this physical tile
  std::vector<std::shared_ptr<storage::Tile>> singleton({ptile});
  std::unique_ptr<LogicalTile> ltile(LogicalTileFactory::WrapTiles(singleton));
  PL_ASSERT(ltile->GetTupleCount() == tile_size);

  SetOutput(ltile.release());

  num_tuples_returned_ += tile_size;

  PL_ASSERT(num_tuples_returned_ <= sort_buffer_.size());

  return true;
}

bool OrderByExecutor::DoSort() {
  PL_ASSERT(children_.size() == 1);
  PL_ASSERT(children_[0] != nullptr);
  PL_ASSERT(!sort_done_);
  PL_ASSERT(executor_context_ != nullptr);

  // Extract all data from child
  while (children_[0]->Execute()) {
    input_tiles_.emplace_back(children_[0]->GetOutput());

    // increase the counter
    num_tuples_get_ += input_tiles_.back()->GetTupleCount();

    // Optimization for ordered output
    if (underling_ordered_ && limit_) {
      LOG_TRACE("underling_ordered and limit both work");
      // We already get enough tuples, break while
      if (num_tuples_get_ >= (limit_offset_ + limit_number_)) {
        LOG_TRACE("num_tuples_get_ (%lu) are enough", num_tuples_get_);
        break;
      }
    }
  }

  /** Number of valid tuples to be sorted. */
  size_t count = 0;
  for (auto &tile : input_tiles_) {
    count += tile->GetTupleCount();
  }

  if (count == 0) return true;

  // Grab data from plan node
  const planner::OrderByPlan &node = GetPlanNode<planner::OrderByPlan>();
  descend_flags_ = node.GetDescendFlags();

  // Extract the schema for sort keys.

  std::unique_ptr<catalog::Schema> physical_schema;
  physical_schema.reset(input_tiles_[0]->GetPhysicalSchema());
  std::vector<catalog::Column> sort_key_columns;
  std::vector<catalog::Column> output_key_columns;
  for (auto id : node.GetSortKeys()) {
    sort_key_columns.push_back(physical_schema->GetColumn(id));
  }
  for (auto id : node.GetOutputColumnIds()) {
    output_key_columns.push_back(physical_schema->GetColumn(id));
  }
  output_column_ids_ = node.GetOutputColumnIds();
  sort_key_tuple_schema_.reset(new catalog::Schema(sort_key_columns));
  output_schema_.reset(new catalog::Schema(output_key_columns));
  auto executor_pool = executor_context_->GetPool();

  // Extract all valid tuples into a single std::vector (the sort buffer)
  sort_buffer_.reserve(count);
  for (oid_t tile_id = 0; tile_id < input_tiles_.size(); tile_id++) {
    for (oid_t tuple_id : *input_tiles_[tile_id]) {
      // Extract the sort key tuple
      std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(sort_key_tuple_schema_.get(), true));
      for (oid_t id = 0; id < node.GetSortKeys().size(); id++) {
        type::Value val =
            (input_tiles_[tile_id]->GetValue(tuple_id, node.GetSortKeys()[id]));
        tuple->SetValue(id, val, executor_pool);
      }
      // Inert the sort key tuple into sort buffer
      sort_buffer_.emplace_back(sort_buffer_entry_t(
          ItemPointer(tile_id, tuple_id), std::move(tuple)));
    }
  }

  PL_ASSERT(count == sort_buffer_.size());

  // If the underlying result has the same order, it is not necessary to sort
  // the result again. Instead, go to the end.
  if (underling_ordered_) {
    LOG_TRACE("underling_ordered works and already get all tuples (%lu)",
              count);
    sort_done_ = true;
    return true;
  }

  // Prepare the compare function
  // Note: This is a less-than comparer, NOT an equality comparer.
  struct TupleComparer {
    TupleComparer(std::vector<bool> &_descend_flags)
        : descend_flags(_descend_flags) {}

    bool operator()(const storage::Tuple *ta, const storage::Tuple *tb) {
      for (oid_t id = 0; id < descend_flags.size(); id++) {
        type::Value va = (ta->GetValue(id));
        type::Value vb = (tb->GetValue(id));
        if (!descend_flags[id]) {
          if (va.CompareLessThan(vb) == CmpBool::TRUE)
            return true;
          else {
            if (va.CompareGreaterThan(vb) == CmpBool::TRUE) return false;
          }
        } else {
          if (vb.CompareLessThan(va) == CmpBool::TRUE)
            return true;
          else {
            if (vb.CompareGreaterThan(va) == CmpBool::TRUE) return false;
          }
        }
      }
      return false;  // Will return false if all keys equal
    }

    std::vector<bool> descend_flags;
  };

  TupleComparer comp(descend_flags_);

  // Finally ... sort it !
  std::sort(
      sort_buffer_.begin(), sort_buffer_.end(),
      [&comp](const sort_buffer_entry_t &a, const sort_buffer_entry_t &b) {
        return comp(a.tuple.get(), b.tuple.get());
      });

  sort_done_ = true;

  return true;
}

}  // namespace executor
}  // namespace peloton
