//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_executor.cpp
//
// Identification: src/executor/projection_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/projection_executor.h"

#include "planner/projection_plan.h"
#include "common/logger.h"
#include "common/internal_types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "common/container_tuple.h"
#include "storage/tile.h"
#include "storage/data_table.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 * @param node  ProjectionNode plan node corresponding to this executor
 */
ProjectionExecutor::ProjectionExecutor(const planner::AbstractPlan *node,
                                       ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool ProjectionExecutor::DInit() {
  // NOTE: We only handle 1 child or no child for now
  PL_ASSERT(children_.size() < 2);

  // Grab settings from plan node
  const planner::ProjectionPlan &node = GetPlanNode<planner::ProjectionPlan>();
  this->project_info_ = node.GetProjectInfo();
  this->schema_ = node.GetSchema();

  return true;
}

/**
 * @brief Create projected tuples based on one or two input.
 * Newly-created physical tiles are needed.
 *
 * @return true on success, false otherwise.
 */
bool ProjectionExecutor::DExecute() {
  PL_ASSERT(project_info_);
  PL_ASSERT(schema_);
  // NOTE: We only handle 1 child or no child for now
  PL_ASSERT(children_.size() < 2);

  if (children_.size() == 0) {
    if (finished_) return false;

    LOG_TRACE("Projection : child 0 ");
    // Create new physical tile where we store projected tuples
    std::shared_ptr<storage::Tile> dest_tile(
        storage::TileFactory::GetTempTile(*schema_, 1));

    // Create projections tuple-at-a-time from original tile
    storage::Tuple *buffer = new storage::Tuple(schema_, true);
    project_info_->Evaluate(buffer, nullptr, nullptr, executor_context_);

    // Insert projected tuple into the new tile
    dest_tile.get()->InsertTuple(0, buffer);

    delete buffer;

    // Wrap physical tile in logical tile and return it
    SetOutput(LogicalTileFactory::WrapTiles({dest_tile}));

    // Return 1 tuple only, set the finished flag to true
    finished_ = true;

    return true;
  }

  else if (children_.size() == 1) {
    LOG_TRACE("Projection : child 1 ");

    // Execute child
    auto status = children_[0]->Execute();
    if (false == status) return false;

    // Get input from child
    std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());
    auto num_tuples = source_tile->GetTupleCount();

    // Create new physical tile where we store projected tuples
    std::shared_ptr<storage::Tile> dest_tile(
        storage::TileFactory::GetTempTile(*schema_, num_tuples));

    // Create projections tuple-at-a-time from original tile
    oid_t new_tuple_id = 0;
    for (oid_t old_tuple_id : *source_tile) {
      storage::Tuple *buffer = new storage::Tuple(schema_, true);
      ContainerTuple<LogicalTile> tuple(source_tile.get(), old_tuple_id);
      project_info_->Evaluate(buffer, &tuple, nullptr, executor_context_);

      // Insert projected tuple into the new tile
      dest_tile.get()->InsertTuple(new_tuple_id, buffer);

      delete buffer;
      new_tuple_id++;
    }

    // Wrap physical tile in logical tile and return it
    SetOutput(LogicalTileFactory::WrapTiles({dest_tile}));

    return true;
  }

  return false;
}

}  // namespace executor
}  // namespace peloton
