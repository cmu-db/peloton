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
class TileIterator : public Iterator<Tile> {
    TileIterator() = delete;

public:
    TileIterator(const DataTable* table)
        : table_(table), tile_itr(0) {
        // More Wu Tang!
    }

    TileIterator(const TileIterator& other)
        : table_(other.table_),
          tile_itr(other.tile_itr) {
        // More Wu Tang!
    }

    /**
     * Updates the given tile so that it points to the next tile in the table.
     * @return true if succeeded. false if no more tuples are there.
     */
    bool Next(Tile& out);

    bool HasNext();

private:
    const DataTable* table_;
    oid_t tile_itr;
};

}  // End storage namespace
}  // End peloton namespace
