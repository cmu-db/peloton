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
  LOG_INFO("********** Nested Loop %s Join executor :: 2 children \n", GetJoinTypeString());

  for(;;){  // Loop until we have non-empty result tile or exit

    LogicalTile* left_tile = nullptr;
    LogicalTile* right_tile = nullptr;

    bool advance_left_child = false;

    if(right_child_done_){  // If we have already retrieved all right child's results in buffer
      LOG_TRACE("Advance the right buffer iterator.");
      assert(!left_result_tiles_.empty());
      assert(!right_result_tiles_.empty());
      right_result_itr_++;
      if(right_result_itr_ >= right_result_tiles_.size()){
        advance_left_child = true;
        right_result_itr_ = 0;
      }
    }
    else { // Otherwise, we must attempt to execute the right child
      if(false == children_[1]->Execute()){
        // right child is finished, no more tiles
        LOG_TRACE("My right child is exhausted.");
        if(right_result_tiles_.empty()){
          assert(left_result_tiles_.empty());
          LOG_TRACE("Right child returns nothing totally. Exit.");
          return false;
        }
        right_child_done_ = true;
        right_result_itr_ = 0;
        advance_left_child = true;
      }
      else { // Buffer the right child's result
        LOG_TRACE("Retrieve a new tile from right child");
        right_result_tiles_.push_back(children_[1]->GetOutput());
        right_result_itr_ = right_result_tiles_.size() - 1;
      }
    }

    if(advance_left_child || left_result_tiles_.empty()){
      assert(0 == right_result_itr_);
      // Need to advance the left child
      if(false == children_[0]->Execute()){
        LOG_TRACE("Left child is exhausted. Returning false.");
        // Left child exhausted.
        // The whole executor is done.
        // Release cur left tile. Clear right child's result buffer and return.
        assert(right_result_tiles_.size() > 0);
        return false;
      }
      else{
        LOG_TRACE("Advance the left child.");
        // Insert left child's result to buffer
        left_result_tiles_.push_back(children_[0]->GetOutput());
      }
    }

    left_tile = left_result_tiles_.back();
    right_tile = right_result_tiles_[right_result_itr_];


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

    /* build the schema given the projection */
    auto output_tile_schema = BuildSchema(left_tile_schema, right_tile_schema);

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

    // Construct position lists for output tile
    // TODO: We don't have to copy position lists for each column,
    // as there are likely duplications of them.
    // But must pay attention to the output schema (see how it is constructed!)
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

    unsigned int removed = 0;
    // Go over every pair of tuples in left and right logical tiles

    // Right Join, Outer Join
    // this set contains row id in right tile that none of the row in left tile matches
    // this set is initialized with all ids in right tile and
    // as the nested loop goes, id in the set are removed if a match is made,
    // After nested looping, ids left are rows with no matching.
    std::unordered_set<oid_t> no_match_rows;
    // only initialize if we are doing right or outer join
    if (join_type_ == JOIN_TYPE_RIGHT || join_type_ == JOIN_TYPE_OUTER) {
      no_match_rows.insert(right_tile->begin(), right_tile->end());
    }
    for(auto left_tile_row_itr : *left_tile){
      bool has_right_match = false;
      for(auto right_tile_row_itr : *right_tile){
        // TODO: OPTIMIZATION : Can split the control flow into two paths -
        // one for Cartesian product and one for join
        // Then, we can skip this branch atleast for the Cartesian product path.

        // Join predicate exists
        if (predicate_ != nullptr) {
          expression::ContainerTuple<executor::LogicalTile> left_tuple(
              left_tile, left_tile_row_itr);
          expression::ContainerTuple<executor::LogicalTile> right_tuple(
              right_tile, right_tile_row_itr);

          // Join predicate is false. Skip pair and continue.
          if (predicate_->Evaluate(&left_tuple, &right_tuple, executor_context_)
                  .IsFalse()) {
            removed++;
            continue;
          }
        }

        // If we are doing right or outer join, remove a matched right row
        if (join_type_ == JOIN_TYPE_RIGHT || join_type_ == JOIN_TYPE_OUTER) {
          no_match_rows.erase(right_tile_row_itr);
        }
        has_right_match = true;

        // Insert a tuple into the output logical tile
        // First, copy the elements in left logical tile's tuple
        for (size_t output_tile_column_itr = 0;
             output_tile_column_itr < left_tile_column_count;
             output_tile_column_itr++) {
          position_lists[output_tile_column_itr].push_back(
              left_tile_position_lists[output_tile_column_itr]
                                      [left_tile_row_itr]);
        }

        // Then, copy the elements in right logical tile's tuple
        for (size_t output_tile_column_itr = 0;
             output_tile_column_itr < right_tile_column_count;
             output_tile_column_itr++) {
          position_lists[left_tile_column_count + output_tile_column_itr]
              .push_back(right_tile_position_lists[output_tile_column_itr]
                                                  [right_tile_row_itr]);
        }
      } // inner loop of NLJ

      if ((join_type_ == JOIN_TYPE_LEFT || join_type_ == JOIN_TYPE_OUTER) && has_right_match) {
        // no right tuple matched, if we are doing left outer join or full outer join
        // we should also emit a tuple in which right parts are null
         for (size_t output_tile_column_itr = 0;
             output_tile_column_itr < left_tile_column_count;
             output_tile_column_itr++) {
          position_lists[output_tile_column_itr].push_back(
              left_tile_position_lists[output_tile_column_itr]
                                      [left_tile_row_itr]);
        }

        // Then, copy the elements in right logical tile's tuple
        for (size_t output_tile_column_itr = 0;
             output_tile_column_itr < right_tile_column_count;
             output_tile_column_itr++) {
          position_lists[left_tile_column_count + output_tile_column_itr]
              .push_back(NULL_OID);
      }
      }
    } // outer loop of NLJ

    // If we are doing right or outer join, for each row in right tile
    // it it has no match in left, we should emit a row whose left parts
    // are null
    if (join_type_ == JOIN_TYPE_RIGHT || join_type_ == JOIN_TYPE_OUTER) {
      for (auto left_null_row_itr : no_match_rows) {
        for (size_t output_tile_column_itr = 0;
             output_tile_column_itr < left_tile_column_count;
             output_tile_column_itr++) {
          position_lists[output_tile_column_itr].push_back(NULL_OID);
        }

        for (size_t output_tile_column_itr = 0;
             output_tile_column_itr < right_tile_column_count;
             output_tile_column_itr++) {
          position_lists[left_tile_column_count + output_tile_column_itr]
              .push_back(right_tile_position_lists[output_tile_column_itr][left_null_row_itr]);
        }
      }
    }


    LOG_INFO("Predicate removed %d rows", removed);

    // Check if we have any matching tuples.
    if (position_lists[0].size() > 0) {
      output_tile->SetPositionListsAndVisibility(std::move(position_lists));
      SetOutput(output_tile.release());
      return true;
    }

    LOG_TRACE("This pair produces empty join result. Loop.");
  } // End large for-loop

}

NestedLoopJoinExecutor::~NestedLoopJoinExecutor(){

  for(auto tile : left_result_tiles_){
    delete tile;
    left_result_tiles_.clear();
  }

  for(auto tile : right_result_tiles_){
    delete tile;
    right_result_tiles_.clear();
  }

}

}  // namespace executor
}  // namespace peloton
