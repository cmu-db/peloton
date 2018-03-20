//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// nested_loop_join_executor.cpp
//
// Identification: src/executor/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/internal_types.h"
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

  // Grab info from plan node and check it
  const planner::NestedLoopJoinPlan &node =
      GetPlanNode<planner::NestedLoopJoinPlan>();

  // Pick out the left and right columns
  const std::vector<oid_t> &join_column_ids_left = node.GetJoinColumnsLeft();
  const std::vector<oid_t> &join_column_ids_right = node.GetJoinColumnsRight();

  // We should first deal with the current result. Otherwise we will cache a lot
  // data which is not good to utilize memory. After that we call child execute.
  // Since is the high level idea, each time we get tile from left, we should
  // finish this tile, and then call child[0] execute for next tile.
  for (;;) {
    //===------------------------------------------------------------------===//
    // Pick left and right tiles
    //===------------------------------------------------------------------===//

    // If we have already retrieved all left child's results in buffer
    if (left_child_done_ == true) {
      LOG_TRACE("Left is done which means all join comparison completes");
      return false;
    }

    // If left tile result is not done, continue the left tuples
    if (!left_tile_done_) {
      // Tuple result
      ContainerTuple<executor::LogicalTile> left_tuple(left_tile_.get(),
                                                       left_tile_row_itr_);

      // Grab the values
      if (!join_column_ids_left.empty() && !join_column_ids_right.empty()) {
        std::vector<type::Value> join_values;
        for (auto column_id : join_column_ids_left) {
          type::Value predicate_value = left_tuple.GetValue(column_id);
          join_values.push_back(predicate_value);
        }

        // Pass the columns and values to right executor
        LOG_TRACE("Update the new value for index predicate");
        children_[1]->UpdatePredicate(join_column_ids_right, join_values);
      }

      // Execute the right child to get the right tile
      if (children_[1]->Execute() == true) {
        LOG_TRACE("Advance the Right child.");
        std::unique_ptr<LogicalTile> right_tile(children_[1]->GetOutput());

        PL_ASSERT(right_tile != nullptr);

        // Construct output result
        auto output_tile =
            BuildOutputLogicalTile(left_tile_.get(), right_tile.get());

        // Build position list
        LogicalTile::PositionListsBuilder pos_lists_builder(left_tile_.get(),
                                                            right_tile.get());

        // Go over every pair of tuples in left and right logical tiles
        for (auto right_tile_row_itr : *right_tile) {
          // Insert a tuple into the output logical tile
          // First, copy the elements in left logical tile's tuple
          LOG_TRACE("Insert a tuple into the output logical tile");

          ContainerTuple<executor::LogicalTile> right_tuple(right_tile.get(),
              right_tile_row_itr);

          if (predicate_ != nullptr) {
            auto eval = predicate_->Evaluate(&left_tuple, &right_tuple,
                                             executor_context_);

            // Join predicate is false. Skip pair and continue.
            if (eval.IsFalse()) {
              LOG_TRACE("Not math join predicate");
              continue;
            }
            LOG_TRACE("Find a tuple with join predicate");
          }
          pos_lists_builder.AddRow(left_tile_row_itr_, right_tile_row_itr);
        }  // Outer loop of NLJ

        // Now current left tile is done
        LOG_TRACE("pos_lists_builder's size : %ld", pos_lists_builder.Size());
        if (pos_lists_builder.Size() > 0) {
          LOG_TRACE("Set output result");
          output_tile->SetPositionListsAndVisibility(
              pos_lists_builder.Release());
          SetOutput(output_tile.release());
          LOG_TRACE("result is : %s", GetOutputInfo()->GetInfo().c_str());
          return true;
        }
        continue;
      }
      // Right table is finished for the current left tuple. move to the next
      else {
        if (!left_child_done_) {
          LOG_TRACE("right child is done, but left is not, so reset right");
          children_[1]->ResetState();

          // When all right table is done, examine whether left tile is done
          // If left tile is done, next loop will directly execute child[0]
          if (left_tile_row_itr_ == left_tile_->GetTupleCount() - 1) {
            LOG_TRACE("left tile is done");
            // Set up flag and go the execute child 0 to get the next tile
            left_tile_done_ = true;
          } else {
            // Move the row to the next one in left tile
            LOG_TRACE("Advance left row");
            left_tile_row_itr_++;

            // Continue the new left row
            continue;
          }
        } else {
          LOG_TRACE("Both left and right child are done");
          right_child_done_ = true;
          return false;
        }
      }
    }  // End handle left tile

    // Otherwise, we must attempt to execute the left child to get a new left
    // tile

    // Left child is finished, no more tiles
    if (children_[0]->Execute() == false) {
      LOG_TRACE("Left child is exhausted.");
      return false;
    }
    // Cache the new tile
    else {
      // Get the left child's result
      LOG_TRACE("Retrieve a new tile from left child");
      left_tile_.reset(children_[0]->GetOutput());

      // Set the flag with init status
      left_tile_done_ = false;
      left_tile_row_itr_ = 0;
    }

    LOG_TRACE("Get a new left tile. Continue the loop.");

  }  // end the very beginning for loop
}

}  // namespace executor
}  // namespace peloton
