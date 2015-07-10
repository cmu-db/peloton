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

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/backend_vm.h"

#include "backend/index/index.h"

#include <string>

namespace peloton {
namespace storage {

/**
 * Base class for all tables
 */
class AbstractTable {
    friend class TileGroup;
    friend class TableFactory;

public:
    
    virtual ~AbstractTable();
    
protected:
        
    // Table constructor
    AbstractTable(catalog::Schema *schema,
                  AbstractBackend *backend,
                  size_t tuples_per_tilegroup);

public:    
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//
    
    void SetSchema(catalog::Schema* given_schema) {
      schema = given_schema;
    }

    const catalog::Schema *GetSchema() const {
        return schema;
    }

    catalog::Schema *GetSchema() {
        return schema;
    }

    AbstractBackend *GetBackend() const {
        return backend;
    }
    
    oid_t GetDatabaseId() const {
        return database_id;
    }
    
    oid_t GetTableId() const {
        return table_id;
    }

    //===--------------------------------------------------------------------===//
    // OPERATIONS
    //===--------------------------------------------------------------------===//

    // add a default unpartitioned tile group to table
    oid_t AddDefaultTileGroup();

    // add a customized tile group to table
    void AddTileGroup(TileGroup *tile_group);

    // NOTE: This must go through the manager's locator
    // This allows us to "TRANSFORM" tile groups atomically
    TileGroup *GetTileGroup(oid_t tile_group_id) const;

    size_t GetTileGroupCount() const;

    // insert tuple in table
    ItemPointer InsertTuple(txn_id_t transaction_id, const Tuple *tuple);

    //===--------------------------------------------------------------------===//
    // UTILITIES
    //===--------------------------------------------------------------------===//
    
    bool CheckNulls(const storage::Tuple *tuple) const;

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
    AbstractBackend *backend;

    // table schema
    catalog::Schema *schema;

    // set of tile groups
    std::vector<oid_t> tile_groups;

    // TODO need some policy ?
    // number of tuples allocated per tilegroup for this table
    size_t tuples_per_tilegroup;
    
    std::mutex table_mutex;
    std::mutex table_unique_index_mutex;
    std::mutex table_reference_table_mutex;

};

} // End storage namespace
} // End peloton namespace


