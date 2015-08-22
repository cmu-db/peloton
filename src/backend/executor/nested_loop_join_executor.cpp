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

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile_factory.h"
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
    planner::AbstractPlan *node, ExecutorContext *executor_context)
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

  left_scan_start = true;
  return true;
}


/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool NestedLoopJoinExecutor::DExecute() {
  LOG_TRACE("********** Nested Loop Join executor :: 2 children \n");

  bool right_scan_end = false;
  // Try to get next tile from RIGHT child
  if (children_[1]->Execute() == false) {
    LOG_TRACE("Did not get right tile \n");
    right_scan_end = true;
  }

  if (right_scan_end == true) {
    LOG_TRACE("Resetting scan for right tile \n");
    children_[1]->Init();
    if (children_[1]->Execute() == false) {
      LOG_ERROR("Did not get right tile on second try\n");
      return false;
    }
  }

  LOG_TRACE("Got right tile \n");

  if (left_scan_start == true || right_scan_end == true) {
    left_scan_start = false;
    // Try to get next tile from LEFT child
    if (children_[0]->Execute() == false) {
      LOG_TRACE("Did not get left tile \n");
      return false;
    }
    LOG_TRACE("Got left tile \n");
  } else {
    LOG_TRACE("Already have left tile \n");
  }

  std::unique_ptr<LogicalTile> left_tile(children_[0]->GetOutput());
  std::unique_ptr<LogicalTile> right_tile(children_[1]->GetOutput());

  // Check the input logical tiles.
  assert(left_tile.get() != nullptr);
  assert(right_tile.get() != nullptr);

  // Construct output logical tile.
  std::unique_ptr<LogicalTile> output_tile(LogicalTileFactory::GetTile());

  auto left_tile_schema = left_tile->GetSchema();
  auto right_tile_schema = right_tile->GetSchema();

  for (auto &col : right_tile_schema) {
    col.position_list_idx += left_tile->GetPositionLists().size();
  }

  /* build the schema given the projection */
  auto output_tile_schema = BuildSchema(left_tile_schema, right_tile_schema);

  // Transfer the base tile ownership
  left_tile->TransferOwnershipTo(output_tile.get());
  right_tile->TransferOwnershipTo(output_tile.get());

  // Set the output logical tile schema
  output_tile->SetSchema(std::move(output_tile_schema));
  // Now, let's compute the position lists for the output tile

  // Cartesian product

  // Add everything from two logical tiles
  auto left_tile_position_lists = left_tile->GetPositionLists();
  auto right_tile_position_lists = right_tile->GetPositionLists();

  // Compute output tile column count
  size_t left_tile_column_count = left_tile_position_lists.size();
  size_t right_tile_column_count = right_tile_position_lists.size();
  size_t output_tile_column_count =
      left_tile_column_count + right_tile_column_count;

  assert(left_tile_column_count > 0);
  assert(right_tile_column_count > 0);

  // Compute output tile row count
  size_t left_tile_row_count = left_tile_position_lists[0].size();
  size_t right_tile_row_count = right_tile_position_lists[0].size();

  // Construct position lists for output tile
  std::vector<std::vector<oid_t>> position_lists;
  for (size_t column_itr = 0; column_itr < output_tile_column_count;
       column_itr++)
    position_lists.push_back(std::vector<oid_t>());

  LOG_TRACE("left col count: %lu, right col count: %lu", left_tile_column_count,
            right_tile_column_count);
  LOG_TRACE("left col count: %lu, right col count: %lu",
            left_tile->GetColumnCount(),
            right_tile->GetColumnCount());
  LOG_TRACE("left row count: %lu, right row count: %lu", left_tile_row_count,
            right_tile_row_count);

  // Go over every pair of tuples in left and right logical tiles
  for (size_t left_tile_row_itr = 0; left_tile_row_itr < left_tile_row_count;
       left_tile_row_itr++) {
    for (size_t right_tile_row_itr = 0;
         right_tile_row_itr < right_tile_row_count; right_tile_row_itr++) {
      // TODO: OPTIMIZATION : Can split the control flow into two paths -
      // one for cartesian product and one for join
      // Then, we can skip this branch atleast for the cartesian product path.

      // Join predicate exists
      if (predicate_ != nullptr) {
        expression::ContainerTuple<executor::LogicalTile> left_tuple(
            left_tile.get(), left_tile_row_itr);
        expression::ContainerTuple<executor::LogicalTile> right_tuple(
            right_tile.get(), right_tile_row_itr);

        // Join predicate is false. Skip pair and continue.
        if (predicate_->Evaluate(&left_tuple, &right_tuple, executor_context_)
                .IsFalse()) {
          continue;
        }
      }

      // Insert a tuple into the output logical tile
      // First, copy the elements in left logical tile's tuple
      for (size_t output_tile_column_itr = 0;
           output_tile_column_itr < left_tile_column_count;
           output_tile_column_itr++) {
        position_lists[output_tile_column_itr].push_back(
            left_tile_position_lists[output_tile_column_itr]
                                    [left_tile_row_itr]);
      }

      // Then, copy the elements in left logical tile's tuple
      for (size_t output_tile_column_itr = 0;
           output_tile_column_itr < right_tile_column_count;
           output_tile_column_itr++) {
        position_lists[left_tile_column_count + output_tile_column_itr]
            .push_back(right_tile_position_lists[output_tile_column_itr]
                                                [right_tile_row_itr]);
      }

      // First, copy the elements in left logical tile's tuple
    }
  }

  for (auto col : position_lists) {
    LOG_TRACE("col");
    for (auto elm : col) {
      (void)elm;  // silent compiler
      LOG_TRACE("elm: %u", elm);
    }
  }

  // Check if we have any matching tuples.
  if (position_lists[0].size() > 0) {
    output_tile->SetPositionListsAndVisibility(std::move(position_lists));
    SetOutput(output_tile.release());
    return true;
  }
  // Try again
  else {
    // If we are out of any more pairs of child tiles to examine,
    // then we will return false earlier in this function.
    // So, we don't have to return false here.
    DExecute();
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
