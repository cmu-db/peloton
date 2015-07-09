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

#include "nodes/nodes.h" // TODO :: REMOVE, just for raw expr

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
              oid_t table_oid,
              size_t tuples_per_tilegroup);

    ~DataTable();

    std::string GetName() const {
        return table_name;
    }
    oid_t  GetId() const {
        return table_oid;
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

    // add the reference to the table with update/delete actions
    void AddReferenceTable(storage::DataTable *table,
                           std::vector<std::string> column_names,
                           catalog::Constraint* constraint,
                           std::string _fk_update_action, 
                           std::string _fk_delete_action );

    // Set the index for PrimaryKey
    void SetPrimaryIndex(index::Index *index);

    // Set raw check expr
    void SetRawCheckExpr(Node* _raw_check_expr);

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
    oid_t table_oid;
    
    // INDEXES
    std::vector<index::Index*> indexes;

    // Index for Unique constraint
    std::vector<index::Index*> unique_indexes;

    // reference tables
    std::vector<storage::DataTable*> reference_tables;
    // foreignkey actions (update, delete)
    std::string fk_update_action;
    std::string fk_delete_action;

    // Index for Primary key
    index::Index* PrimaryKey_Index = nullptr;

    // Raw check expr
    Node* raw_check_expr;

};


} // End storage namespace
} // End peloton namespace


