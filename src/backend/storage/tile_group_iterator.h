//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_group_iterator.h
//
// Identification: src/backend/storage/tile_group_iterator.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "backend/common/types.h"
#include "backend/common/iterator.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// TileGroup Iterator
//===--------------------------------------------------------------------===//

class DataTable;
class TileGroup;

/**
 * Iterator for table which goes over all active tiles.
 * FIXME: This is not thread-safe or transactional!
 **/
class TileGroupIterator : public Iterator<std::shared_ptr<TileGroup>> {
  TileGroupIterator() = delete;

 public:
  TileGroupIterator(const DataTable *table) :
    table_(table),
    tile_group_itr_(0) {
    // More Wu Tang!
  }

  TileGroupIterator(const TileGroupIterator &other)
      : table_(other.table_),
        tile_group_itr_(other.tile_group_itr_) {
    // More Wu Tang!
  }

  /**
   * Updates the given tile so that it points to the next tile in the table.
   * @return true if succeeded. false if no more tuples are there.
   */
  bool Next(std::shared_ptr<TileGroup> &tileGroup);

  bool HasNext();

 private:
  // Table
  const DataTable *table_;

  // Tile group iterator
  oid_t tile_group_itr_;
};

}  // End storage namespace
}  // End peloton namespace
