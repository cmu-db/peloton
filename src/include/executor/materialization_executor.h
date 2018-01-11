//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// materialization_executor.h
//
// Identification: src/include/executor/materialization_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <vector>

#include "common/internal_types.h"
#include "executor/abstract_executor.h"

namespace peloton {

namespace planner {
class AbstractPlan;
}

namespace storage {
class Tile;
}

namespace executor {
class LogicalTile;

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class MaterializationExecutor : public AbstractExecutor {
 public:
  MaterializationExecutor(const MaterializationExecutor &) = delete;
  MaterializationExecutor &operator=(const MaterializationExecutor &) = delete;
  MaterializationExecutor(MaterializationExecutor &&) = delete;
  MaterializationExecutor &operator=(MaterializationExecutor &&) = delete;

  // If node is null, then we will create a default node in DExecute()
  // Else, we will apply the column mapping on the logical tile based on node.
  explicit MaterializationExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  void GenerateTileToColMap(
      const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
      LogicalTile *source_tile,
      std::unordered_map<storage::Tile *, std::vector<oid_t>> &tile_to_cols);

  void MaterializeByTiles(
      LogicalTile *source_tile,
      const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
      const std::unordered_map<storage::Tile *, std::vector<oid_t>> &
          tile_to_cols,
      storage::Tile *dest_tile,
      const peloton::LayoutType peloton_layout_mode = peloton::LayoutType::ROW);

  LogicalTile *Physify(LogicalTile *source_tile);
  std::unordered_map<oid_t, oid_t> BuildIdentityMapping(
      const catalog::Schema *schema);
};

}  // namespace executor
}  // namespace peloton
