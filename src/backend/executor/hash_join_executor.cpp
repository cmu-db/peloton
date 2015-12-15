/*-------------------------------------------------------------------------
 *
 * hash_join.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/hash_join_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <vector>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/hash_join_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for nested loop join executor.
 * @param node Nested loop join node corresponding to this executor.
 */
HashJoinExecutor::HashJoinExecutor(const planner::AbstractPlan *node,
                                     ExecutorContext *executor_context)
    : AbstractJoinExecutor(node, executor_context) {
}

bool HashJoinExecutor::DInit() {
  assert(children_.size() == 2);

  auto status = AbstractJoinExecutor::DInit();
  if (status == false)
    return status;

  assert(children_[1]->GetRawNode()->GetPlanNodeType() == PLAN_NODE_TYPE_HASH);

  hash_executor_ = reinterpret_cast<HashExecutor*>(children_[1]);

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool HashJoinExecutor::DExecute() {
  LOG_INFO("********** Hash Join executor :: 2 children \n");


  /* Hash right tiles and get right tiles */
  if (!hashed_) {
    while (children_[1]->Execute()) {
      right_tiles_.emplace_back(children_[1]->GetOutput());
    }
    hashed_ = true;
  }

  if (right_tiles_.size() == 0) {
    LOG_INFO("Did not get right tiles");
    return false;
  }

  // Try to get next tile from LEFT child
  if (children_[0]->Execute() == false) {
    LOG_INFO("Did not get left tile \n");
    return false;
  }

  std::unique_ptr<LogicalTile> left(children_[0]->GetOutput());
  LOG_INFO("Got left tile \n");

  LogicalTile *left_tile = left.get();
  LogicalTile *right_tile = right_tiles_.back().get();

  // Check the input logical tiles.
  assert(left_tile != nullptr);
  assert(right_tile != nullptr);

  // Construct output logical tile.
  std::unique_ptr<LogicalTile> output_tile(LogicalTileFactory::GetTile());

  auto left_tile_schema = left_tile->GetSchema();
  auto right_tile_schema = right_tile->GetSchema();

  for (auto &col : right_tile_schema) {
    col.position_list_idx += left_tile->GetPositionLists().size();
  }

  // Build the schema given the projection
  auto output_tile_schema = BuildSchema(left_tile_schema, right_tile_schema);

  // Set the output logical tile schema
  output_tile->SetSchema(std::move(output_tile_schema));

  // Get position list from two logical tiles
  auto &left_tile_position_lists = left_tile->GetPositionLists();
  auto &right_tile_position_lists = right_tile->GetPositionLists();

  // Compute the output logical tile column count
  size_t left_tile_column_count = left_tile_position_lists.size();
  size_t right_tile_column_count = right_tile_position_lists.size();
  size_t output_tile_column_count = left_tile_column_count
      + right_tile_column_count;

  assert(left_tile_column_count > 0);
  assert(right_tile_column_count > 0);

  // Construct position lists for output tile
  std::vector<std::vector<oid_t> > position_lists;
  for (size_t column_itr = 0; column_itr < output_tile_column_count; column_itr++)
    position_lists.push_back(std::vector<oid_t>());

  // Get the hash table from the hash executor
  auto &htable = hash_executor_->GetHashTable();
  auto &hashed_col_ids = hash_executor_->GetHashKeyIds();

  // Go over the left tile
  for (auto left_tile_itr : *left_tile) {
     const expression::ContainerTuple<executor::LogicalTile> left_tuple(
         left_tile, left_tile_itr, &hashed_col_ids);

     // Find matching tuples in the hash table built on top of the right table
     auto &set = htable.at(left_tuple);

     // Go over the matching right tuples
     for (auto &location : set) {
       auto &right_tile_position_lists = right_tiles_[location.first]->GetPositionLists();
       for (size_t output_tile_column_itr = 0;
            output_tile_column_itr < left_tile_column_count;
            output_tile_column_itr++) {
          position_lists[output_tile_column_itr].push_back(
              left_tile_position_lists[output_tile_column_itr][left_tile_itr]);
        }

        // Then, copy the elements in the left logical tile's tuple
        for (size_t output_tile_column_itr = 0;
            output_tile_column_itr < right_tile_column_count;
            output_tile_column_itr++) {

          position_lists[left_tile_column_count + output_tile_column_itr]
                         .push_back(right_tile_position_lists[output_tile_column_itr][location.second]);
        }
     }
  }

  // Build output logical tile if we have any matching tuples.
  if (position_lists[0].size() > 0) {
    output_tile->SetPositionListsAndVisibility(std::move(position_lists));
    SetOutput(output_tile.release());
    return true;
  }

  return false;
}

}  // namespace executor
}  // namespace peloton
