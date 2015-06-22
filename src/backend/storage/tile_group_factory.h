/*-------------------------------------------------------------------------
 *
 * tile_group_factory.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/manager.h"
#include "backend/storage/abstract_table.h"
#include "backend/storage/tile_group.h"

#include <string>

namespace nstore {
namespace storage {

/**
 * Super Awesome TileGroupFactory!!
 */
class TileGroupFactory {
public:
    TileGroupFactory();
    virtual ~TileGroupFactory();

    static TileGroup *GetTileGroup(oid_t database_id, oid_t table_id, oid_t tile_group_id,
                                   AbstractTable* table,
                                   AbstractBackend* backend,
                                   const std::vector<catalog::Schema>& schemas,
                                   int tuple_count);



};

} // End storage namespace
} // End nstore namespace


