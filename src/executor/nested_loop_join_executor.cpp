/*-------------------------------------------------------------------------
 *
 * nested_loop_join.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/nested_loop_join_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "executor/nested_loop_join_executor.h"

#include <vector>

#include "common/types.h"
#include "common/logger.h"
#include "executor/logical_tile_factory.h"
#include "expression/abstract_expression.h"
#include "expression/container_tuple.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for nested loop join executor.
 * @param node Nested loop join node corresponding to this executor.
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(planner::AbstractPlanNode *node)
: AbstractExecutor(node, nullptr) {
}

/**
 * @brief Do some basic checks and create the schema for the output logical tiles.
 * @return true on success, false otherwise.
 */
bool NestedLoopJoinExecutor::DInit() {
  assert(children_.size() == 2);

  // Grab data from plan node.
  const planner::NestedLoopJoinNode &node = GetNode<planner::NestedLoopJoinNode>();

  // NOTE: predicate can be null for cartesian product
  predicate_ = node.GetPredicate();

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying join predicate.
 * @return true on success, false otherwise.
 */
bool NestedLoopJoinExecutor::DExecute() {

  LOG_TRACE("Nested Loop Join executor :: 2 children \n");

  // Try to get next tile from LEFT child
  if (children_[0]->Execute() == false) {
    return false;
  }

  // Try to get next tile from RIGHT child
  if (children_[1]->Execute() == false) {

    // Check if reinit helps
    children_[1]->Init();

    if (children_[1]->Execute() == false) {
      return false;
    }
  }

  std::unique_ptr<LogicalTile> left_tile(children_[0]->GetOutput());
  std::unique_ptr<LogicalTile> right_tile(children_[1]->GetOutput());

  // Check the input logical tiles.
  assert(left_tile.get() != nullptr);
  assert(right_tile.get() != nullptr);

  // Construct output logical tile.
  std::unique_ptr<LogicalTile> output_tile(LogicalTileFactory::GetTile());

  auto output_tile_schema = left_tile.get()->GetSchema();
  auto right_tile_schema = right_tile.get()->GetSchema();

  output_tile_schema.insert(output_tile_schema.end(),
                            right_tile_schema.begin(), right_tile_schema.end());

  // Set the output logical tile schema
  output_tile.get()->SetSchema(output_tile_schema);

  // Now, let's compute the position lists for the output tile

  // Join predicate exists
  if (predicate_ != nullptr) {

  }
  // Cartesian product
  else {

    // Add everything from two logical tiles
    auto left_tile_position_lists = left_tile.get()->GetPositionLists();
    auto right_tile_position_lists = right_tile.get()->GetPositionLists();

    // Compute output tile column count
    size_t left_tile_column_count = left_tile_position_lists.size();
    size_t right_tile_column_count = right_tile_position_lists.size();
    assert(left_tile_column_count > 0);
    assert(right_tile_column_count > 0);

    // Compute output tile row count
    size_t left_tile_row_count = left_tile_position_lists[0].size();
    size_t right_tile_row_count = right_tile_position_lists[0].size();
    size_t output_tile_row_count = left_tile_row_count * right_tile_row_count;

    // Construct position lists for output tile
    std::vector< std::vector<oid_t> > position_lists;
    for(size_t column_itr = 0; column_itr < right_tile_column_count; column_itr++)
      position_lists.push_back(std::vector<oid_t>(INVALID_OID, output_tile_row_count));

    // Go over every pair of tuples in left and right logical tiles
    size_t output_tile_row_itr = 0;
    for(size_t left_tile_row_itr = 0 ; left_tile_row_itr < left_tile_row_count; left_tile_row_itr++){
      for(size_t right_tile_row_itr = 0 ; right_tile_row_itr < right_tile_row_count; right_tile_row_itr++){

        // And, for each pair, insert a tuple into the output logical tile

        // First, copy the elements in left logical tile's tuple
        for(size_t output_tile_column_itr = 0 ;
            output_tile_column_itr < left_tile_column_count;
            output_tile_column_itr++){
          position_lists[output_tile_column_itr][output_tile_row_itr] =
              left_tile_position_lists[output_tile_column_itr][left_tile_row_itr];
        }

        // Then, copy the elements in left logical tile's tuple
        for(size_t output_tile_column_itr = 0 ;
            output_tile_column_itr < right_tile_column_count;
            output_tile_column_itr++){
          position_lists[left_tile_column_count + output_tile_column_itr][output_tile_row_itr] =
              right_tile_position_lists[output_tile_column_itr][left_tile_row_itr];
        }

        output_tile_row_itr++;
      }
    }

    assert(output_tile_row_itr == output_tile_row_count);

    SetOutput(output_tile.release());
  }

  return true;
}

} // namespace executor
} // namespace nstore


