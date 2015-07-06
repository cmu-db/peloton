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

    // add the unique index to the table
    void AddUniqueIndex(index::Index *index);

    // add the reference(foreignkey) to the table
    void AddReferenceTable(storage::DataTable *table);

    // Set the index for PrimaryKey
    void SetPrimaryIndex(index::Index *index);

    // Get the PrimaryKey index
    index::Index* GetPrimaryIndex();

    inline bool ishasPrimaryKey(){
        if( PrimaryKey_Index != nullptr )
          return true;
        else
          return false;
    }

    inline bool ishasUnique(){
        if( unique_indexes.size() > 0 )
          return true;
        else
          return false;
    }

    inline bool ishasReferenceTable(){
        if( reference_tables.size() > 0 )
          return true;
        else
          return false;
    }

    inline size_t GetIndexCount() const {
        return indexes.size();
    }

    inline size_t GetUniqueIndexCount() const {
        return unique_indexes.size();
    }

    inline size_t GetReferenceTableCount() const {
        return reference_tables.size();
    }

    inline index::Index *GetIndex(oid_t index_id) const {
        assert(index_id < indexes.size());
        return indexes[index_id];
    }

    inline index::Index *GetUniqueIndex(oid_t index_id) const {
        assert(index_id < unique_indexes.size());
        return unique_indexes[index_id];
    }

    inline storage::DataTable *GetReferenceTable(oid_t table_id) const {
        assert(table_id < reference_tables.size());
        return reference_tables[table_id];
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

    // Index for Unique constraint
    std::vector<index::Index*> unique_indexes;

    // reference tables
    std::vector<storage::DataTable*> reference_tables;

    // Index for Primary key
    index::Index* PrimaryKey_Index = nullptr;

};


} // End storage namespace
} // End peloton namespace


