//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nested_loop_join_executor.cpp
//
// Identification: src/backend/executor/nested_loop_join_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>
#include <unordered_set>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/executor/nested_loop_join_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for nested loop join executor.
 * @param node Nested loop join node corresponding to this executor.
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(
    const planner::AbstractPlan *node, ExecutorContext *executor_context)
    : AbstractJoinExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and create the schema for the output logical
 * tiles.
 * @return true on success, false otherwise.
 */
bool NestedLoopJoinExecutor::DInit() {
  auto status = AbstractJoinExecutor::DInit();
  if (status == false) {
    return status;
  }

  assert(right_result_tiles_.empty());
  right_child_done_ = false;
  right_result_itr_ = 0;

  assert(left_result_tiles_.empty());

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool NestedLoopJoinExecutor::DExecute() {
  LOG_INFO("********** Nested Loop %s Join executor :: 2 children ",
           GetJoinTypeString());

  // Loop until we have non-empty result tile or exit
  for (;;) {
    // Build outer join output when done
    if (left_child_done_ && right_child_done_) {
      return BuildOuterJoinOutput();
    }

    //===--------------------------------------------------------------------===//
    // Pick right and left tiles
    //===--------------------------------------------------------------------===//

    LogicalTile *left_tile = nullptr;
    LogicalTile *right_tile = nullptr;

    bool advance_left_child = false;

    // If we have already retrieved all right child's results in buffer
    if (right_child_done_ == true) {
      LOG_TRACE("Advance the right buffer iterator.");

      assert(!left_result_tiles_.empty());
      assert(!right_result_tiles_.empty());
      right_result_itr_++;

      if (right_result_itr_ >= right_result_tiles_.size()) {
        advance_left_child = true;
        right_result_itr_ = 0;
      }

    }
    // Otherwise, we must attempt to execute the right child
    else {
      // Right child is finished, no more tiles
      if (children_[1]->Execute() == false) {
        LOG_TRACE("Right child is exhausted.");

        if (right_result_tiles_.empty()) {
          assert(left_result_tiles_.empty());
          LOG_TRACE("Right child returned nothing. Exit.");
          return false;
        }

        right_child_done_ = true;
        right_result_itr_ = 0;
        advance_left_child = true;
      }
      // Buffer the right child's result
      else {
        LOG_TRACE("Retrieve a new tile from right child");
        BufferRightTile(children_[1]->GetOutput());
        right_result_itr_ = right_result_tiles_.size() - 1;
      }
    }

    if (advance_left_child == true || left_result_tiles_.empty()) {
      assert(right_result_itr_ == 0);

      // Left child is finished, no more tiles
      if (children_[0]->Execute() == false) {
        LOG_TRACE("Left child is exhausted. Returning false.");

        // Left child exhausted.
        // Release cur left tile. Clear right child's result buffer and return.
        assert(right_result_tiles_.size() > 0);
        left_child_done_ = true;

        return BuildOuterJoinOutput();
      }
      // Buffer the left child's result
      else {
        LOG_TRACE("Advance the left child.");
        BufferLeftTile(children_[0]->GetOutput());
      }
    }

    left_tile = left_result_tiles_.back().get();
    right_tile = right_result_tiles_[right_result_itr_].get();

    //===--------------------------------------------------------------------===//
    // Build Join Tile
    //===--------------------------------------------------------------------===//

    // Build output logical tile
    auto output_tile = BuildOutputLogicalTile(left_tile, right_tile);

    // Build position lists
    LogicalTile::PositionListsBuilder pos_lists_builder(left_tile, right_tile);

    // Go over every pair of tuples in left and right logical tiles
    for (auto left_tile_row_itr : *left_tile) {
      bool has_right_match = false;

      for (auto right_tile_row_itr : *right_tile) {
        // Join predicate exists
        if (predicate_ != nullptr) {
          expression::ContainerTuple<executor::LogicalTile> left_tuple(
              left_tile, left_tile_row_itr);
          expression::ContainerTuple<executor::LogicalTile> right_tuple(
              right_tile, right_tile_row_itr);

          // Join predicate is false. Skip pair and continue.
          if (predicate_->Evaluate(&left_tuple, &right_tuple, executor_context_)
                  .IsFalse()) {
            continue;
          }
        }

        RecordMatchedRightRow(right_result_itr_, right_tile_row_itr);

        // For Left and Full Outer Join
        has_right_match = true;

        // Insert a tuple into the output logical tile
        // First, copy the elements in left logical tile's tuple
        pos_lists_builder.AddRow(left_tile_row_itr, right_tile_row_itr);
      }  // Inner loop of NLJ

      // For Left and Full Outer Join
      if (has_right_match) {
        RecordMatchedLeftRow(left_result_tiles_.size() - 1, left_tile_row_itr);
      }

    }  // Outer loop of NLJ

    // Check if we have any join tuples.
    if (pos_lists_builder.Size() > 0) {
      output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
      SetOutput(output_tile.release());
      return true;
    }

    LOG_TRACE("This pair produces empty join result. Continue the loop.");
  }
}

}  // namespace executor
}  // namespace peloton
