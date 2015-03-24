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

#include <vector>

#include "common/types.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "storage/tuple.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// LogicalTile
//===--------------------------------------------------------------------===//

/**
 * Represents a LogicalTile.
 *
 * Tiles are only instantiated via LogicalTileFactory.
 */
class LogicalTile {

 public:

  LogicalTile(catalog::Catalog *catalog, id_t base_tile_count)
 : catalog(catalog), base_tile_count(base_tile_count) {
  }

  // Add a tuple set to the container
  void AppendTupleSet(std::vector<catalog::ItemPointer> tuple_set);

  // Add a tuple to the container at the given offset
  void AppendTuple(id_t offset, catalog::ItemPointer tuple);

  // Get the tuple from given tile at the given tuple offset
  storage::Tuple *GetTuple(id_t tile_offset, id_t offset);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation of this tile
  friend std::ostream& operator<<(std::ostream& os, const LogicalTile& logical_tile);

 protected:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // container of tuple pointer sets
  std::vector<std::vector<catalog::ItemPointer> > tuple_set_container;

  // catalog for tile mapping
  catalog::Catalog *catalog;

  // number of base tiles
  id_t base_tile_count;

};


} // End executor namespace
} // End nstore namespace

