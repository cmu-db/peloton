/*-------------------------------------------------------------------------
 *
 * logical_tile.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/logical_tile.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <memory>
#include <vector>

#include "common/types.h"
#include "executor/logical_schema.h"
#include "storage/tuple.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// LogicalTile
//===--------------------------------------------------------------------===//

/**
 * @brief Represents a LogicalTile.
 *
 * Tiles are only instantiated via LogicalTileFactory.
 */
class LogicalTile {

 public:
  LogicalTile(LogicalSchema *schema);

  // Add a tuple to the container at the given offset
  void AppendPositionTuple(std::vector<id_t> const &tuple);

  /** @brief Get the tuple from given tile at the given tuple offset */
  storage::Tuple *GetTuple(id_t column_id, id_t tuple_id);

  // Get the value from given tile at the given tuple offset and column offset
  Value GetValue(id_t column_id, id_t tuple_id);

  // Get a string representation of this tile
  friend std::ostream& operator<<(std::ostream& os, const LogicalTile& logical_tile);

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // logical tile schema
  // We use a different schema representation from physical tile as they contain very different
  // metadata.
  std::unique_ptr<LogicalSchema> schema_;

  // container of position tuples
  std::vector<std::vector<id_t> > position_tuple_list_;
};


} // namespace executor
} // namespace nstore

