/**
 * @brief Tile group implementation of tuple interface that is used by
 *        expression system.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <cassert>

#include "common/types.h"
#include "common/value.h"
#include "expression/tuple.h"
#include "storage/tile_group.h"

namespace nstore {
namespace expression {

class TileGroupTuple : public Tuple {
 public:
  TileGroupTuple(const TileGroupTuple &) = default;
  TileGroupTuple& operator=(const TileGroupTuple &) = default;
  TileGroupTuple(TileGroupTuple &&) = default;
  TileGroupTuple& operator=(TileGroupTuple &&) = default;

  TileGroupTuple(storage::TileGroup *tile_group, id_t tuple_id)
    : tile_group_(tile_group),
      tuple_id_(tuple_id) {
  }

  const Value GetValue(id_t column_id) const override {
    assert(tile_group_ != nullptr);
    return tile_group_->GetValue(tuple_id_, column_id);
  }

  inline char *GetData() const override {
    //TODO This is used by tuple address expression. Wtf is that?
    assert(false);
    return nullptr;
  }

 private:
  /** @brief Underlying tile group behind this tuple interface. */
  storage::TileGroup *tile_group_;

  /**
   * @brief Tuple id of tuple in tile group that this wrapper is pretending
   *        to be.
   */
  const id_t tuple_id_;

};

} // namespace expression
} // namespace nstore
