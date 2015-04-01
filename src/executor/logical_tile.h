/**
 * @brief Header for logical tile.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <vector>

#include "common/types.h"
#include "common/value.h"

namespace nstore {

namespace storage {
  class Tuple;
}

namespace executor {
class LogicalSchema;

class LogicalTile {

 public:
  LogicalTile(std::unique_ptr<LogicalSchema> schema);

  LogicalSchema *schema();

  void AppendPositionTuple(std::vector<id_t> const &tuple);

  storage::Tuple *GetTuple(id_t column_id, id_t tuple_id);

  Value GetValue(id_t column_id, id_t tuple_id);

  friend std::ostream& operator<<(
      std::ostream& os, const LogicalTile& logical_tile);

 private:
  /**
   * @brief Schema of this logical tile.
   *
   * We use a different schema representation from physical tile as they
   * contain very different metadata.
   */
  std::unique_ptr<LogicalSchema> schema_;

  /**
   * @brief List of positions tuples.
   *
   * Each position tuple contains the tuple ids of the fields in their origin
   * base tiles.
   */
  std::vector<std::vector<id_t> > position_tuple_list_;
};


} // namespace executor
} // namespace nstore

