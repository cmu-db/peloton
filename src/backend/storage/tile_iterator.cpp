/*-------------------------------------------------------------------------
 *
 * tile_iterator.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/storage/tile_iterator.h"

namespace peloton {
namespace storage {

bool TileIterator::Next(Tile& out) {
    if (HasNext()) {
        auto nextTile = table_->GetTileGroup(tile_itr);
        return (true);
    }
    return (false);
}

bool TileIterator::HasNext() {
    return (tile_itr < table_->GetTileGroupCount());
}
    
    
}  // End storage namespace
}  // End peloton namespace
