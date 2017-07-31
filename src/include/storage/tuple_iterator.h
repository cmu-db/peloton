//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_iterator.h
//
// Identification: src/include/storage/tuple_iterator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "common/iterator.h"
#include "storage/tuple.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Tuple Iterator
//===--------------------------------------------------------------------===//

/**
 * Iterator for tile which goes over all active tuples within
 * a single tile.
 **/
class TupleIterator : public Iterator<Tuple> {
  TupleIterator() = delete;

 public:
  TupleIterator(const Tile *tile)
      : data(tile->data),
        tile(tile),
        tuple_itr(0),
        tuple_length(tile->tuple_length) {
    tile_group_header = tile->tile_group_header;
  }

  TupleIterator(const TupleIterator &other)
      : data(other.data),
        tile(other.tile),
        tile_group_header(other.tile_group_header),
        tuple_itr(other.tuple_itr),
        tuple_length(other.tuple_length) {}

  /**
   * Updates the given tuple so that it points to the next tuple in the table.
   * @return true if succeeded. false if no more tuples are there.
   */
  bool Next(Tuple &out) {
    if (HasNext()) {
      out.Move(data + (tuple_itr * tuple_length));
      tuple_itr++;

      return true;
    }

    return false;
  }

  bool HasNext() { return (tuple_itr < tile->GetActiveTupleCount()); }

  oid_t GetLocation() const { return tuple_itr; }

 private:
  // Base tile data
  char *data;

  const Tile *tile;

  const TileGroupHeader *tile_group_header;

  // Iterator over tile data
  oid_t tuple_itr;

  oid_t tuple_length;
};

}  // namespace storage
}  // namespace peloton
