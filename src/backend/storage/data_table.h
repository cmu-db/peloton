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

#include "backend/storage/abstract_table.h"
#include "backend/storage/backend_vm.h"

#include "backend/index/index.h"

#include <string>

namespace peloton {
namespace storage {

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
    DataTable(const catalog::Schema *schema,
              AbstractBackend *backend,
              std::string table_name,
              size_t tuples_per_tilegroup);

    ~DataTable();

    std::string GetName() const {
        return table_name;
    }
    
    //===--------------------------------------------------------------------===//
    // OPERATIONS
    //===--------------------------------------------------------------------===//
    
    // insert tuple in table
    ItemPointer InsertTuple(txn_id_t transaction_id, const Tuple *tuple, bool update = false);
    
    //===--------------------------------------------------------------------===//
    // INDEXES
    //===--------------------------------------------------------------------===//

    // add an index to the table
    void AddIndex(index::Index *index);
    
    inline size_t GetIndexCount() const {
        return indexes.size();
    }

    inline index::Index *GetIndex(oid_t index_id) const {
        assert(index_id < indexes.size());
        return indexes[index_id];
    }

    void InsertInIndexes(const storage::Tuple *tuple, ItemPointer location);
    bool TryInsertInIndexes(const storage::Tuple *tuple, ItemPointer location);

    void DeleteInIndexes(const storage::Tuple *tuple);

    bool CheckNulls(const storage::Tuple *tuple) const;


    //===--------------------------------------------------------------------===//
    // UTILITIES
    //===--------------------------------------------------------------------===//

    // Get a string representation of this table
    friend std::ostream& operator<<(std::ostream& os, const DataTable& table);

protected:

    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//

    std::string table_name;
    
    // INDEXES
    std::vector<index::Index*> indexes;

};


} // End storage namespace
} // End peloton namespace


