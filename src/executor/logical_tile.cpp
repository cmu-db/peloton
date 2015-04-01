/**
 * @brief Implementation of logical tile.
 *
 * This abstraction is used to implement late materialization of tiles in the
 * execution engine.
 * Tiles are only instantiated via LogicalTileFactory.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/logical_tile.h"

#include <cassert>
#include <iostream>

#include "executor/logical_schema.h"
#include "storage/tile.h"

namespace nstore {
namespace executor {

/** @brief Constructor for logical tile. */
LogicalTile::LogicalTile(std::unique_ptr<LogicalSchema> schema)
  : schema_(std::move(schema)) {
}

/**
 * @brief Returns schema of this tile.
 *
 * @return Pointer to logical schema of this tile.
 */
LogicalSchema *LogicalTile::schema() {
  return schema_.get();
}

/**
 * @brief Adds a position tuple to this tile.
 * @param tuple Vector of tuple ids (positions).
 *
 * TODO Right now the caller has to ensure that the tuple is
 * in the right order. Maybe we should create a PositionTuple class to manage
 * that logic instead of requiring executors to be aware of this...
 */
void LogicalTile::AppendPositionTuple(std::vector<id_t> const &tuple) {
  assert(tuple.size() == schema_->NumValidCols());

  // First we ensure that the columns of the position tuple align with
  // the schema (because some columns might exist but be invalidated).
  std::vector<id_t> aligned_tuple;
  int tuple_idx = 0;

  for (unsigned int schema_idx = 0;
      schema_idx < schema_->NumCols();
      schema_idx++) {
    if (schema_->IsValid(schema_idx)) {
      aligned_tuple.push_back(tuple[tuple_idx]);
      tuple_idx++;
    } else {
      aligned_tuple.push_back(INVALID_ID);
    }

  }

  // Add aligned tuple to tuple list.
  position_tuple_list_.push_back(aligned_tuple);
}

/**
 * @brief Get the tuple from the base tile that contains the specified field.
 * @param column_id Column id of the specified field.
 * @param tuple_id Tuple id of the specified field (row/position).
 *
 * @return Pointer to copy of tuple from base tile.
 */
storage::Tuple *LogicalTile::GetTuple(id_t column_id, id_t tuple_id) {
  assert(tuple_id < position_tuple_list_.size());
  assert(schema_->IsValid(column_id));

  id_t base_tuple_id = position_tuple_list_[tuple_id][column_id];
  storage::Tile *base_tile = schema_->GetBaseTile(column_id);

  // Get a copy of the tuple from the underlying physical tile.
  storage::Tuple *tuple = base_tile->GetTuple(base_tuple_id);

  return tuple;
}

/**
 * @brief Get the value at the specified field.
 * @param column_id Column id of the specified field.
 * @param tuple_id Tuple id of the specified field (row/position).
 *
 * @return Value at the specified field.
 */
Value LogicalTile::GetValue(id_t column_id, id_t tuple_id) {
  assert(tuple_id < position_tuple_list_.size());
  assert(schema_->IsValid(column_id));

  id_t base_tuple_id = position_tuple_list_[tuple_id][column_id];
  storage::Tile *base_tile = schema_->GetBaseTile(column_id);
  id_t base_column_id = schema_->GetOriginColumnId(column_id);

  Value value = base_tile->GetValue(base_tuple_id, base_column_id);

  return value;
}

/** @brief Returns a string representation of this tile. */
std::ostream& operator<<(std::ostream& os, const LogicalTile& logical_tile) {

  os << "\t-----------------------------------------------------------\n";

  os << "\tLOGICAL TILE\n";

  os << "\t-----------------------------------------------------------\n";
  os << "\tSCHEMA\n";
  os << (*logical_tile.schema_);

  os << "\t-----------------------------------------------------------\n";
  os << "\tROW MAPPING\n";

  for(auto position_tuple : logical_tile.position_tuple_list_){
    os << "\t" ;
    for(auto pos : position_tuple) {
      os << " Position: " << pos << ", ";
    }
    os << "\n" ;
  }

  os << "\t-----------------------------------------------------------\n";

  return os;
}

} // End executor namespace
} // End nstore namespace
