//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_executor.cpp
//
// Identification: src/executor/hash_join_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/internal_types.h"
#include "common/logger.h"
#include "executor/logical_tile_factory.h"
#include "executor/hash_join_executor.h"
#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"
#include "common/container_tuple.h"

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
  PL_ASSERT(children_.size() == 2);

  auto status = AbstractJoinExecutor::DInit();
  if (status == false) return status;

  PL_ASSERT(children_[1]->GetRawNode()->GetPlanNodeType() ==
            PlanNodeType::HASH);

  hash_executor_ = reinterpret_cast<HashExecutor *>(children_[1]);

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool HashJoinExecutor::DExecute() {
  LOG_TRACE("********** Hash Join executor :: 2 children \n");

  // Loop until we have non-empty result tile or exit
  for (;;) {
    // Check if we have any buffered output tiles
    if (buffered_output_tiles.empty() == false) {
      auto output_tile = buffered_output_tiles.front();
      SetOutput(output_tile);
      buffered_output_tiles.pop_front();
      return true;
    }

    // Build outer join output when done
    if (left_child_done_ == true) {
      return BuildOuterJoinOutput();
    }

    //===------------------------------------------------------------------===//
    // Pick right and left tiles
    //===------------------------------------------------------------------===//

    // Get all the tiles from RIGHT child
    if (right_child_done_ == false) {
      while (children_[1]->Execute()) {
        BufferRightTile(children_[1]->GetOutput());
      }
      right_child_done_ = true;
    }

    // Get next tile from LEFT child
    if (children_[0]->Execute() == false) {
      LOG_TRACE("Did not get left tile \n");
      left_child_done_ = true;
      continue;
    }

    BufferLeftTile(children_[0]->GetOutput());
    LOG_TRACE("Got left tile \n");

    if (right_result_tiles_.size() == 0) {
      LOG_TRACE("Did not get any right tiles \n");
      return BuildOuterJoinOutput();
    }

    LogicalTile *left_tile = left_result_tiles_.back().get();

    //===------------------------------------------------------------------===//
    // Build Join Tile
    //===------------------------------------------------------------------===//

    // Get the hash table from the hash executor
    auto &hash_table = hash_executor_->GetHashTable();
    std::vector<const expression::AbstractExpression *> left_hashed_cols;
    this->GetPlanNode<planner::HashJoinPlan>().GetLeftHashKeys(left_hashed_cols);

    std::vector<oid_t> left_hashed_col_ids;
    for (auto &hashkey : left_hashed_cols) {
      PL_ASSERT(hashkey->GetExpressionType() == ExpressionType::VALUE_TUPLE);
      auto tuple_value =
          reinterpret_cast<const expression::TupleValueExpression *>(hashkey);
      left_hashed_col_ids.push_back(tuple_value->GetColumnId());
    }

    oid_t prev_tile = INVALID_OID;
    std::unique_ptr<LogicalTile> output_tile;
    LogicalTile::PositionListsBuilder pos_lists_builder;

    // Go over the left tile
    for (auto left_tile_itr : *left_tile) {
      const ContainerTuple<executor::LogicalTile> left_tuple(
          left_tile, left_tile_itr, &left_hashed_col_ids);

      // Find matching tuples in the hash table built on top of the right table
      auto right_tuples = hash_table.find(left_tuple);

      if (right_tuples != hash_table.end()) {
    	// Not yet supported due to assertion in gettomg right_tuples->first
    	if (predicate_ != nullptr) {
    		auto eval = predicate_->Evaluate(&left_tuple, &right_tuples->first,
					executor_context_);
			if (eval.IsFalse())
				continue;
    	}

        RecordMatchedLeftRow(left_result_tiles_.size() - 1, left_tile_itr);

        // Go over the matching right tuples
        for (auto &location : right_tuples->second) {
          // Check if we got a new right tile itr
          if (prev_tile != location.first) {
            // Check if we have any join tuples
            if (pos_lists_builder.Size() > 0) {
              LOG_TRACE("Join tile size : %lu \n", pos_lists_builder.Size());
              output_tile->SetPositionListsAndVisibility(
                  pos_lists_builder.Release());
              buffered_output_tiles.push_back(output_tile.release());
            }

            // Get the logical tile from right child
            LogicalTile *right_tile = right_result_tiles_[location.first].get();

            // Build output logical tile
            output_tile = BuildOutputLogicalTile(left_tile, right_tile);

            // Build position lists
            pos_lists_builder =
                LogicalTile::PositionListsBuilder(left_tile, right_tile);

            pos_lists_builder.SetRightSource(
                &right_result_tiles_[location.first]->GetPositionLists());
          }

          // Add join tuple
          pos_lists_builder.AddRow(left_tile_itr, location.second);

          RecordMatchedRightRow(location.first, location.second);

          // Cache prev logical tile itr
          prev_tile = location.first;
        }
      }
    }

    // Check if we have any join tuples
    if (pos_lists_builder.Size() > 0) {
      LOG_TRACE("Join tile size : %lu \n", pos_lists_builder.Size());
      output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
      buffered_output_tiles.push_back(output_tile.release());
    }

    // Check if we have any buffered output tiles
    if (buffered_output_tiles.empty() == false) {
      auto output_tile = buffered_output_tiles.front();
      SetOutput(output_tile);
      buffered_output_tiles.pop_front();

      return true;
    } else {
      // Try again
      continue;
    }
  }
}

}  // namespace executor
}  // namespace peloton
