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
#include "backend/storage/abstract_table.h"
#include "backend/storage/backend_vm.h"
#include "backend/index/index.h"

#include "nodes/nodes.h" // TODO :: REMOVE, just for raw expr

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
    DataTable(catalog::Schema *schema,
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

    
    // Add an index to the table
    bool AddIndex( index::Index *index , oid_t index_oid);

    void AddReferenceTable( catalog::ReferenceTableInfo *referenceTableInfo );

    inline index::Index *GetIndex(oid_t index_offset) const {
        assert(index_offset < indexes.size());
        return indexes[index_offset];
    }

    index::Index* GetIndexByOid(oid_t index_oid );

    inline bool ishasPrimaryKey(){
      if( primary_key_count > 0 )
        return true;
      else
        return false;
    }

    inline bool ishasUnique(){
      if( unique_count > 0 )
        return true;
      else
        return false;
    }

    inline bool ishasReferenceTable(){
      if( reference_table_infos.size() > 0 )
        return true;
      else
        return false;
    }

    inline size_t GetIndexCount() const {
      return indexes.size();
    }

    inline size_t GetUniqueIndexCount() const {
      return unique_count;
    }

    inline size_t GetReferenceTableCount() const {
        return reference_table_infos.size();
    }

    storage::DataTable *GetReferenceTable(int offset) ;
    catalog::ReferenceTableInfo *GetReferenceTableInfo(int offset) ;

    // insert tuple in table
    ItemPointer InsertTuple( txn_id_t transaction_id, const Tuple *tuple, bool update = false );

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

    // table name and oid
    std::string table_name;
    oid_t table_oid;
    
    // INDEXES
    std::vector<index::Index*> indexes;

    // Primary key and unique key count
    unsigned int primary_key_count = 0;
    unsigned int unique_count = 0;

    // Reference tables
    std::vector<catalog::ReferenceTableInfo*> reference_table_infos;

    // Convert index oid to index address
    oid_t_to_index_ptr index_oid_to_address;

};


} // End storage namespace
} // End peloton namespace


