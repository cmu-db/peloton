/**
 * @brief Header file for materialization executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "executor/abstract_executor.h"

namespace nstore {

namespace planner {
  class AbstractPlanNode;
}

namespace storage {
  class Tile;
}

namespace executor {
class LogicalTile;

class MaterializationExecutor : public AbstractExecutor {
 public:
  explicit MaterializationExecutor(const planner::AbstractPlanNode *node);

 protected:
  bool Init();

  LogicalTile *GetNextTile();

  bool CleanUp();

 private:
  void GenerateTileToColMap(
    std::unordered_map<id_t, id_t> &old_to_new_cols,
    LogicalTile *source_tile,
    std::unordered_map<storage::Tile *, std::vector<id_t> > &tile_to_cols);

  void MaterializeByTiles(
      LogicalTile *source_tile,
      std::unordered_map<id_t, id_t> &old_to_new_cols,
      const
      std::unordered_map<storage::Tile *, std::vector<id_t> > &tile_to_cols,
      storage::Tile *dest_tile);
};

} // namespace executor
} // namespace nstore
