//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// order_by_executor.cpp
//
// Identification: src/backend/executor/order_by_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/common/pool.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/order_by_executor.h"

#include "../planner/order_by_plan.h"
#include "backend/storage/tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 * @param node  OrderByNode plan node corresponding to this executor
 */
OrderByExecutor::OrderByExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

bool OrderByExecutor::DInit() {
  assert(children_.size() == 1);

  sort_done_ = false;
  num_tuples_returned_ = 0;

  return true;
}

bool OrderByExecutor::DExecute() {
  LOG_TRACE("Order By executor \n");

  if (!sort_done_) DoSort();

  if (!(num_tuples_returned_ < sort_buffer_.size())) {
    return false;
  }

  assert(sort_done_);
  assert(input_schema_.get());
  assert(input_tiles_.size() > 0);

  // Returned tiles must be newly created physical tiles,
  // which have the same physical schema as input tiles.
  size_t tile_size = std::min(size_t(DEFAULT_TUPLES_PER_TILEGROUP),
                              sort_buffer_.size() - num_tuples_returned_);
  storage::Tile *ptile = storage::TileFactory::GetTile(
      INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID, nullptr,
      *input_schema_, nullptr, tile_size);
  for (size_t id = 0; id < tile_size; id++) {
    oid_t source_tile_id =
        sort_buffer_[num_tuples_returned_ + id].item_pointer.block;
    oid_t source_tuple_id =
        sort_buffer_[num_tuples_returned_ + id].item_pointer.offset;
    // Insert a physical tuple into physical tile
    for (oid_t col = 0; col < input_schema_->GetColumnCount(); col++) {
      ptile->SetValue(
          input_tiles_[source_tile_id]->GetValue(source_tuple_id, col), id,
          col);
    }
  }

  // Create an owner wrapper of this physical tile
  std::vector<storage::Tile *> singleton({ptile});
  std::unique_ptr<LogicalTile> ltile(
      LogicalTileFactory::WrapTiles(singleton));
  assert(ltile->GetTupleCount() == tile_size);

  SetOutput(ltile.release());

  num_tuples_returned_ += tile_size;

  assert(num_tuples_returned_ <= sort_buffer_.size());

  return true;
}

bool OrderByExecutor::DoSort() {
  assert(children_.size() == 1);
  assert(children_[0] != nullptr);
  assert(!sort_done_);

  // Extract all data from child
  while (children_[0]->Execute()) {
    input_tiles_.emplace_back(children_[0]->GetOutput());
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
  input_schema_.reset(input_tiles_[0]->GetPhysicalSchema());
  std::vector<catalog::Column> sort_key_columns;
  for (auto id : node.GetSortKeys()) {
    sort_key_columns.push_back(input_schema_->GetColumn(id));
  }
  sort_key_tuple_schema_.reset(new catalog::Schema(sort_key_columns));

  std::unique_ptr<VarlenPool> pool(new VarlenPool());

  // Extract all valid tuples into a single std::vector (the sort buffer)
  sort_buffer_.reserve(count);
  for (oid_t tile_id = 0; tile_id < input_tiles_.size(); tile_id++) {
    for (oid_t tuple_id : *input_tiles_[tile_id]) {
      // Extract the sort key tuple
      std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(sort_key_tuple_schema_.get(), true));
      for (oid_t id = 0; id < node.GetSortKeys().size(); id++) {
        tuple->SetValue(id,
                                input_tiles_[tile_id]->GetValue(tuple_id, node.GetSortKeys()[id]),
                                pool.get());
      }
      // Inert the sort key tuple into sort buffer
      sort_buffer_.emplace_back(sort_buffer_entry_t(
          ItemPointer(tile_id, tuple_id), std::move(tuple)));
    }
  }

  assert(count == sort_buffer_.size());

  // Prepare the compare function
  // Note: This is a less-than comparer, NOT an equality comparer.
  struct TupleComparer {
    TupleComparer(std::vector<bool> &_descend_flags)
        : descend_flags(_descend_flags) {}

    bool operator()(const storage::Tuple *ta, const storage::Tuple *tb) {
      for (oid_t id = 0; id < descend_flags.size(); id++) {
        if (!descend_flags[id]) {
          if (ta->GetValue(id).OpLessThan(tb->GetValue(id)).IsTrue()) {
            return true;
          } else if (ta->GetValue(id)
                         .OpGreaterThan(tb->GetValue(id))
                         .IsTrue()) {
            return false;
          }
        } else {
          if (tb->GetValue(id).OpLessThan(ta->GetValue(id)).IsTrue()) {
            return true;
          } else if (tb->GetValue(id)
                         .OpGreaterThan(ta->GetValue(id))
                         .IsTrue()) {
            return false;
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

} /* namespace executor */
} /* namespace peloton */
