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
  TileGroupIterator(const DataTable *table) : table_(table), tileGroupItr_(0) {
    // More Wu Tang!
  }

  TileGroupIterator(const TileGroupIterator &other)
      : table_(other.table_), tileGroupItr_(other.tileGroupItr_) {
    // More Wu Tang!
  }

  /**
   * Updates the given tile so that it points to the next tile in the table.
   * @return true if succeeded. false if no more tuples are there.
   */
  bool Next(std::shared_ptr<TileGroup> &tileGroup);

  bool HasNext();

 private:
  const DataTable *table_;
  oid_t tileGroupItr_;
};

}  // End storage namespace
}  // End peloton namespace
