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

bool TileIterator::Next(std::shared_ptr<Tile> &tile) {
    if (HasNext()) {
        auto nextTile = table_->GetTileGroup(tileItr_);
        tile.reset(nextTile);
        return (true);
    }
    return (false);
}

bool TileIterator::HasNext() {
    return (tileItr_ < table_->GetTileGroupCount());
}
    
    
}  // End storage namespace
}  // End peloton namespace
