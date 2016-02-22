//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_join_executor.cpp
//
// Identification: src/backend/executor/hash_join_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
 * @brief Constructor for hash join executor.
 * @param node Hash join node corresponding to this executor.
 */
HashJoinExecutor::HashJoinExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context)
    : AbstractJoinExecutor(node, executor_context) {}

bool HashJoinExecutor::DInit() {
  assert(children_.size() == 2);

  auto status = AbstractJoinExecutor::DInit();
  if (status == false) return status;

  assert(children_[1]->GetRawNode()->GetPlanNodeType() == PLAN_NODE_TYPE_HASH);

  hash_executor_ = reinterpret_cast<HashExecutor *>(children_[1]);

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool HashJoinExecutor::DExecute() {
  // Loop until we have non-empty result join logical tile or exit

  // Build outer join output when done

  //===--------------------------------------------------------------------===//
  // Pick right and left tiles
  //===--------------------------------------------------------------------===//

  // Get all the logical tiles from RIGHT child

  // Get next logical tile from LEFT child

  //===--------------------------------------------------------------------===//
  // Build Join Tile
  //===--------------------------------------------------------------------===//

  // Build output join logical tile

  // Build position lists

  // Get the hash table from the hash executor
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

	  // Build output logical tile
	  auto output_tile = BuildOutputLogicalTile(left_tile, right_tile);

	  // Build position lists
	  auto position_lists = BuildPostitionLists(left_tile, right_tile);

	  // Get position list from two logical tiles
	  auto &left_tile_position_lists = left_tile->GetPositionLists();
	  auto &right_tile_position_lists = right_tile->GetPositionLists();
	  size_t left_tile_column_count = left_tile_position_lists.size();
	  size_t right_tile_column_count = right_tile_position_lists.size();

  // Get the hash table from the hash executor
  auto &htable = hash_executor_->GetHashTable();

  // auto &hashed_col_ids = hash_executor_->GetHashKeyIds();
  const planner::HashJoinPlan &hj_plan_node = GetPlanNode<planner::HashJoinPlan>();
  std::vector<oid_t> hashed_col_ids = hj_plan_node.GetOuterHashIds();

  for (oid_t item : hashed_col_ids) {
	  oid_t i = item;
	  std::cout << i;
  }

  // Go over the left tile
  for (auto left_tile_itr : *left_tile) {
     const expression::ContainerTuple<executor::LogicalTile> left_tuple(
         left_tile, left_tile_itr, &hashed_col_ids);

     // Find matching tuples in the hash table built on top of the right table
     //auto &set = htable.at(left_tuple);
     //HashMapType::const_iterator got = htable.find(left_tuple);
     auto got = htable.find(left_tuple);
     if ( got == htable.end() ) continue;

     auto set = got->second;
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
