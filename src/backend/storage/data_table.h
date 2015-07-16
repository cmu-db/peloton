/*-------------------------------------------------------------------------
 *
 * data_table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/bridge/bridge.h"
#include "backend/catalog/foreign_key.h"
#include "backend/catalog/table.h"
#include "backend/storage/abstract_table.h"
#include "backend/storage/backend_vm.h"
#include "backend/index/index.h"

#include <string>

namespace peloton {
namespace storage {

typedef tbb::concurrent_unordered_map<oid_t, index::Index*> oid_t_to_index_ptr;

//===--------------------------------------------------------------------===//
// DataTable
//===--------------------------------------------------------------------===//

/**
 * Represents a group of tile groups logically vertically contiguous.
 *
 * <Tile Group 1>
 * <Tile Group 2>
 * ...
 * <Tile Group n>
 *
 */
class DataTable : public AbstractTable {
    friend class TileGroup;
    friend class TableFactory;

    DataTable() = delete;
    DataTable(DataTable const&) = delete;

public:
    // Table constructor
    DataTable(catalog::Table *catalog_table,
              catalog::Schema *schema,
              AbstractBackend *backend,
              std::string table_name,
              oid_t table_oid,
              size_t tuples_per_tilegroup);

    ~DataTable();

   //===--------------------------------------------------------------------===//
    // OPERATIONS
    //===--------------------------------------------------------------------===//

    std::string GetName() const {
        return table_name;
    }

    oid_t  GetId() const {
        return table_oid;
    }

    // insert tuple in table
    ItemPointer InsertTuple( txn_id_t transaction_id, const Tuple *tuple, bool update = false );

    void InsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

    bool TryInsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

    void DeleteInIndexes(const storage::Tuple *tuple);

    bool CheckNulls(const storage::Tuple *tuple) const;

    // Add an index to the table
    bool AddIndex(index::Index *index, oid_t index_oid);

    // Add a foreign key to the table
    void AddForeignKey(catalog::ForeignKey *foreign_key);

    //===--------------------------------------------------------------------===//
     // ACCESSORS
     //===--------------------------------------------------------------------===//

    index::Index *GetIndex(oid_t index_offset) const;

    index::Index* GetIndexByOid(oid_t index_oid);

    catalog::ForeignKey *GetForeignKey(oid_t key_offset) ;

    oid_t GetIndexCount() const {
      return catalog_table->GetIndexCount();
    }

    oid_t GetForeignKeyCount() const {
      return catalog_table->GetForeignKeyCount();
    }

    //===--------------------------------------------------------------------===//
    // UTILITIES
    //===--------------------------------------------------------------------===//

    // Get a string representation of this table
    friend std::ostream& operator<<(std::ostream& os, const DataTable& table);

protected:

    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//

    // table name and oid
    std::string table_name;

    // catalog info
    oid_t table_oid;
    
    // Catalog table
    catalog::Table *catalog_table;
};


} // End storage namespace
} // End peloton namespace


