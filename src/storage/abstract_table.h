/*-------------------------------------------------------------------------
 *
 * abstract_table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/manager.h"
#include "storage/tile_group.h"
#include "storage/backend_vm.h"

#include "index/index.h"

#include <string>

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Abstract Table
//===--------------------------------------------------------------------===//

/**
 * Base class for tables
 */
class AbstractTable {
    friend class TileGroup;
    friend class TableFactory;

public:
    
    virtual ~AbstractTable();
    
protected:
        
    // Table constructor
    AbstractTable(catalog::Schema *schema, Backend *backend);

public:    
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//
    
    inline catalog::Schema *GetSchema() const{
        return schema;
    }
    
    inline Backend *GetBackend() const {
        return backend;
    }
    
    inline oid_t GetDatabaseId() const {
        return database_id;
    }
    
    inline oid_t GetTableId() const {
        return table_id;
    }

    //===--------------------------------------------------------------------===//
    // OPERATIONS
    //===--------------------------------------------------------------------===//

    // add a default unpartitioned tile group to table
    oid_t AddDefaultTileGroup();

    // add a customized tile group to table
    void AddTileGroup(TileGroup *tile_group);

    // add an index to the table
    void AddIndex(index::Index *index);

    // NOTE: This must go through the manager's locator
    // This allows us to "TRANSFORM" tile groups atomically
    TileGroup *GetTileGroup(oid_t tile_group_id) const;

    size_t GetTileGroupCount() const;

    // insert tuple in table
    ItemPointer InsertTuple(txn_id_t transaction_id, const Tuple *tuple, bool update = false);

    //===--------------------------------------------------------------------===//
    // UTILITIES
    //===--------------------------------------------------------------------===//
    
    bool AbstractTable::CheckNulls(const storage::Tuple *tuple) const;

    // Get a string representation of this table
    friend std::ostream& operator<<(std::ostream& os, const AbstractTable& table);

protected:

    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//

    // Catalog information
    oid_t database_id;
    oid_t table_id;

    // backend
    Backend *backend;

    // table schema
    catalog::Schema *schema;

    // set of tile groups
    std::vector<oid_t> tile_groups;

    // TODO need some policy ?
    // number of tuples allocated per tilegroup for this table
    size_t tuples_per_tilegroup = 1000;

};

} // End storage namespace
} // End nstore namespace


