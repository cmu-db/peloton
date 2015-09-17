//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// projection_executor.cpp
//
// Identification: src/backend/executor/projection_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/projection_executor.h"

#include "../planner/projection_plan.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 * @param node  ProjectionNode plan node corresponding to this executor
 */
ProjectionExecutor::ProjectionExecutor(planner::AbstractPlan *node,
                                       ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool ProjectionExecutor::DInit() {
  assert(children_.size() == 1 || children_.size() == 2);

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
  assert(project_info_);
  assert(schema_);
  assert(children_.size() == 1);

  // NOTE: We only handle 1 child for now
  if (children_.size() == 1) {
    LOG_INFO("Projection : child 1 \n");

    // Execute child
    auto status = children_[0]->Execute();
    if (false == status) return false;

    // Get input from child
    std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());
    auto num_tuples = source_tile->GetTupleCount();

    // Create new physical tile where we store projected tuples
    std::unique_ptr<storage::Tile> dest_tile(
        storage::TileFactory::GetTempTile(*schema_, num_tuples));

    // Create projections tuple-at-a-time from original tile
    oid_t new_tuple_id = 0;
    for (oid_t old_tuple_id : *source_tile) {
      storage::Tuple *buffer = new storage::Tuple(schema_, true);
      expression::ContainerTuple<LogicalTile> tuple(source_tile.get(),
                                                    old_tuple_id);
      project_info_->Evaluate(buffer, &tuple, nullptr, executor_context_);

      // Insert projected tuple into the new tile
      dest_tile->InsertTuple(new_tuple_id, buffer);

      delete buffer;
      new_tuple_id++;
    }

    // Wrap physical tile in logical tile and return it
    bool own_base_tile = true;
    SetOutput(
        LogicalTileFactory::WrapTiles({dest_tile.release()}, own_base_tile));

    return true;
  }

  return false;
}

} /* namespace executor */
} /* namespace peloton */
