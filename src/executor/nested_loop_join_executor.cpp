//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// nested_loop_join_executor.cpp
//
// Identification: src/executor/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>
#include <unordered_set>

#include "type/types.h"
#include "common/logger.h"
#include "executor/nested_loop_join_executor.h"
#include "executor/executor_context.h"
#include "executor/index_scan_executor.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/index_scan_plan.h"
#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"
#include "common/container_tuple.h"

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

  PL_ASSERT(right_result_tiles_.empty());
  right_child_done_ = false;
  right_result_itr_ = 0;

  PL_ASSERT(left_result_tiles_.empty());

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 *
 * ExecutorContext is set when executing IN+NestLoop. For example:
 * select * from Foo1 where age IN (select id from Foo2 where name='mike');
 * Here:
 * "select id from Foo2 where name='mike'" is transformed as left child.
 * "select * from Foo1 where age " is the right child.
 * "IN" is transformed as a execute context, in NestLoop
 * We put the results of left child in executor_context using NestLoop, and the
 * right child can execute using this context. Otherwise, the right child can't
 * execute. And there is no predicate_ for IN+NestLoop
 *
 * For now, we only set this context for IN operator. Normally, the right child
 * has a complete query that can execute without the context, and we use
 *predicate_
 * to join the left and right result.
 *
 */

// For this version, the work flow is that we first lookup the left table, and
// use the result to lookup right table. If left table is done that means right
// table is also done. So we only keep the left_child_done_ as the sign.
bool NestedLoopJoinExecutor::DExecute() {
  LOG_TRACE("********** Nested Loop %s Join executor :: 2 children ",
            GetJoinTypeString());

  // Loop until we have non-empty result tile or exit
  for (;;) {
    //===------------------------------------------------------------------===//
    // Pick left and right tiles
    //===------------------------------------------------------------------===//

    LogicalTile *left_tile = nullptr;
    LogicalTile *right_tile = nullptr;

    // If we have already retrieved all left child's results in buffer
    if (left_child_done_ == true) {
      LOG_TRACE("Advance the left buffer iterator.");

      PL_ASSERT(!right_result_tiles_.empty());
      left_result_itr_++;

      if (left_result_itr_ >= left_result_tiles_.size()) {
        left_result_itr_ = 0;
      }
    }
    // Otherwise, we must attempt to execute the left child
    else {
      // Left child is finished, no more tiles
      if (children_[0]->Execute() == false) {
        LOG_TRACE("Left child is exhausted.");

        left_child_done_ = true;
        left_result_itr_ = 0;
      }
      // Buffer the left child's result
      else {
        LOG_TRACE("Retrieve a new tile from left child");
        BufferLeftTile(children_[0]->GetOutput());
        left_result_itr_ = left_result_tiles_.size() - 1;
      }
    }

    if (left_result_tiles_.empty() && !left_child_done_) {
      // If there is no result for left lookup, continue the next tile
      LOG_TRACE("Left is empty continue the left.");
      continue;
    } else if (left_child_done_) {
      LOG_TRACE("Left_child_done, and return the result.");
      return BuildOuterJoinOutput();
    }

    // We already checked whether results are empty
    left_tile = left_result_tiles_[left_result_itr_].get();

    //===------------------------------------------------------------------===//
    // Look up the right table using the left result
    //===------------------------------------------------------------------===//

    // Grab info from plan node and check it
    const planner::NestedLoopJoinPlan &node =
        GetPlanNode<planner::NestedLoopJoinPlan>();

    // Pick out the left and right columns
    const std::vector<oid_t> &join_column_ids_left = node.GetJoinColumnsLeft();
    const std::vector<oid_t> &join_column_ids_right =
        node.GetJoinColumnsRight();

    for (auto left_tile_row_itr : *left_tile) {
      // Tuple result
      expression::ContainerTuple<executor::LogicalTile> left_tuple(
          left_tile, left_tile_row_itr);

      // Grab the values
      std::vector<type::Value> join_values;
      for (auto column_id : join_column_ids_left) {
        type::Value predicate_value = left_tuple.GetValue(column_id);
        join_values.push_back(predicate_value);
      }

      // Pass the columns and values to right executor
      children_[1]->UpdatePredicate(join_column_ids_right, join_values);

      // return if right tile is empty
      if (right_child_done_ && right_result_tiles_.empty()) {
        return BuildOuterJoinOutput();
      }

      // Right child is finished, no more tiles
      for (;;) {
        if (children_[1]->Execute() == true) {
          LOG_TRACE("Advance the Right child.");
          BufferRightTile(children_[1]->GetOutput());

          // return if left tile is empty
          if (left_child_done_ && left_result_tiles_.empty()) {
            return BuildOuterJoinOutput();
          }
        }
        // Right is finished
        else {
          if (!left_child_done_) {
            children_[1]->ResetState();
          } else {
            right_child_done_ = true;
          }
          break;
        }
      }  // End for
    }    // Buffered all results

    // Return result
    if (left_child_done_ && right_result_tiles_.empty()) {
      LOG_TRACE("All done, and return the result.");
      return BuildOuterJoinOutput();
    } else if (right_result_tiles_.empty()) {
      LOG_TRACE("Right is empty, continue the left.");
      continue;
    }

    // We already checked whether results are empty
    right_tile = right_result_tiles_.back().get();

    //===------------------------------------------------------------------===//
    // Build Join Tile
    //===------------------------------------------------------------------===//
    LOG_TRACE("Build output logical tile.");

    // Build output logical tile
    auto output_tile = BuildOutputLogicalTile(left_tile, right_tile);

    // Build position lists
    LogicalTile::PositionListsBuilder pos_lists_builder(left_tile, right_tile);

    // Go over every pair of tuples in left and right logical tiles
    for (auto right_tile_row_itr : *right_tile) {
      bool has_left_match = false;

      for (auto left_tile_row_itr : *left_tile) {
        // Join predicate exists
        if (predicate_ != nullptr) {
          expression::ContainerTuple<executor::LogicalTile> left_tuple(
              left_tile, left_tile_row_itr);
          expression::ContainerTuple<executor::LogicalTile> right_tuple(
              right_tile, right_tile_row_itr);

          LOG_INFO("LEFT:%s\n", left_tuple.GetValue(0).GetInfo().c_str());
          LOG_INFO("RIght:%s\n", right_tuple.GetValue(0).GetInfo().c_str());

          // Join predicate is false. Skip pair and continue.
          auto eval = predicate_->Evaluate(&left_tuple, &right_tuple,
                                           executor_context_);
          if (eval.IsFalse()) {
            LOG_INFO("Not math join predicate");
            continue;
          }
          LOG_INFO("Find a tuple with join predicate");
        }

        RecordMatchedLeftRow(left_result_itr_, left_tile_row_itr);

        // For Left and Full Outer Join
        has_left_match = true;

        // Insert a tuple into the output logical tile
        // First, copy the elements in left logical tile's tuple
        pos_lists_builder.AddRow(left_tile_row_itr, right_tile_row_itr);
      }  // Inner loop of NLJ

      // For Right and Full Outer Join
      if (has_left_match) {
        RecordMatchedRightRow(right_result_tiles_.size() - 1,
                              right_tile_row_itr);
      }

    }  // Outer loop of NLJ

    // Check if we have any join tuples.
    if (pos_lists_builder.Size() > 0) {
      output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
      SetOutput(output_tile.release());
      return true;
    }

    LOG_TRACE("This pair produces empty join result. Continue the loop.");
  }  // end the very beginning for loop
}

}  // namespace executor
}  // namespace peloton
