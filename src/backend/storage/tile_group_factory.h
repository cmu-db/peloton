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

#include "catalog/manager.h"
#include "storage/abstract_table.h"
#include "storage/tile_group.h"

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
                                   Backend* backend,
                                   const std::vector<catalog::Schema>& schemas,
                                   int tuple_count);



};

} // End storage namespace
} // End nstore namespace


