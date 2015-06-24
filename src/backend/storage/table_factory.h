/*-------------------------------------------------------------------------
 *
 * table_factory.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/data_table.h"

#include <iostream>
#include <map>
#include <string>

namespace nstore {
namespace storage {

/**
 * Magic Table Factory!!
 */
class TableFactory {
    
public:

    /**
     * For a given Schema, instantiate a DataTable object and return it
     */
    static DataTable* GetDataTable(oid_t database_oid,
                                   catalog::Schema *schema,
                                   std::string table_name,
                                   size_t tuples_per_tile_group_count = DEFAULT_TUPLES_PER_TILEGROUP);

    /**
     * For a given table name, drop the table from database
     */
    static bool DropDataTable(oid_t database_oid, std::string table_name);

};

} // End storage namespace
} // End nstore namespace


