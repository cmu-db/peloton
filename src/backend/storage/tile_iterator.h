/*-------------------------------------------------------------------------
 *
 * table_iterator.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <memory>

#include "backend/common/iterator.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Iterator
//===--------------------------------------------------------------------===//
    
class DataTable;

/**
 * Iterator for table which goes over all active tiles.
 * FIXME: This is not thread-safe or transactional!
 **/
class TileIterator : public Iterator<std::shared_ptr<Tile>> {
    TileIterator() = delete;

public:
    TileIterator(const DataTable* table)
        : table_(table), tileItr_(0) {
        // More Wu Tang!
    }

    TileIterator(const TileIterator& other)
        : table_(other.table_),
          tileItr_(other.tileItr_) {
        // More Wu Tang!
    }

    /**
     * Updates the given tile so that it points to the next tile in the table.
     * @return true if succeeded. false if no more tuples are there.
     */
    bool Next(std::shared_ptr<Tile> &tile);

    bool HasNext();

private:
    const DataTable* table_;
    oid_t tileItr_;
};

}  // End storage namespace
}  // End peloton namespace
