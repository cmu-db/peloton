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

#include <memory>
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
  LOG_INFO("********** Nested Loop %s Join executor :: 2 children ",
           GetJoinTypeString());

  // Grab info from plan node and check it
  const planner::NestedLoopJoinPlan &node =
      GetPlanNode<planner::NestedLoopJoinPlan>();

  // Pick out the left and right columns
  const std::vector<oid_t> &join_column_ids_left = node.GetJoinColumnsLeft();
  const std::vector<oid_t> &join_column_ids_right = node.GetJoinColumnsRight();

  std::unique_ptr<LogicalTile> output_tile = nullptr;
  std::unique_ptr<LogicalTile::PositionListsBuilder> pos_lists_builder =
      nullptr;

  // Loop until we have non-empty result tile or exit
  for (;;) {
    //===------------------------------------------------------------------===//
    // Pick left and right tiles
    //===------------------------------------------------------------------===//

    // If we have already retrieved all left child's results in buffer
    if (left_child_done_ == true) {
      LOG_INFO("Left is done which means all join comparison completes");
      return false;
    }

    // Otherwise, we must attempt to execute the left child

    // Left child is finished, no more tiles
    if (children_[0]->Execute() == false) {
      LOG_INFO("Left child is exhausted.");
      return false;
    }

    // Get the left child's result
    LOG_INFO("Retrieve a new tile from left child");
    auto left_tile = children_[0]->GetOutput();

    // Left result should not be empty if execute returns true
    PL_ASSERT(left_tile != nullptr);

    // Iterate the left tile to lookup right table
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
      LOG_INFO("Update the new value for index predicate");
      children_[1]->UpdatePredicate(join_column_ids_right, join_values);

      // Execute right child
      for (;;) {
        if (children_[1]->Execute() == true) {
          LOG_INFO("Advance the Right child.");
          auto right_tile = children_[1]->GetOutput();

          PL_ASSERT(right_tile != nullptr);

          // Construct output result. Only construct it for once
          if (output_tile == nullptr) {
            output_tile = BuildOutputLogicalTile(left_tile, right_tile);
            pos_lists_builder.reset(
                new LogicalTile::PositionListsBuilder(left_tile, right_tile));
          }

          // Go over every pair of tuples in left and right logical tiles
          for (auto right_tile_row_itr : *right_tile) {
            // Insert a tuple into the output logical tile
            // First, copy the elements in left logical tile's tuple
            LOG_INFO("Insert a tuple into the output logical tile");
            pos_lists_builder->AddRow(left_tile_row_itr, right_tile_row_itr);
          }  // Outer loop of NLJ
        }
        // Right table is finished for the current left tuple
        else {
          if (!left_child_done_) {
            children_[1]->ResetState();
          } else {
            right_child_done_ = true;
          }
          break;
        }
      }  // End for right table for current left tuple
    }    // End for current left tile

    // Now current left tile is done
    LOG_INFO("pos_lists_builder's size : %ld", pos_lists_builder->Size());
    if (pos_lists_builder->Size() > 0) {
      LOG_INFO("Set output result");
      output_tile->SetPositionListsAndVisibility(
          (*pos_lists_builder).Release());
      SetOutput(output_tile.release());
      LOG_INFO("result is : %s", GetOutputInfo()->GetInfo().c_str());
      return true;
    }

    LOG_INFO("This pair produces empty join result. Continue the loop.");

  }  // end the very beginning for loop
}
}  // namespace executor
}  // namespace peloton
