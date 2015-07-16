/*-------------------------------------------------------------------------
 *
l* database.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "tbb/concurrent_unordered_map.h"
#include "backend/common/types.h"
#include "backend/storage/data_table.h"
#include "backend/common/logger.h"

#include <iostream>

namespace peloton {
namespace storage {

class Database;

typedef tbb::concurrent_unordered_map<oid_t, Database*> oid_t_to_database_ptr;
typedef tbb::concurrent_unordered_map<std::string, oid_t> string_to_oid_t;
typedef tbb::concurrent_unordered_map<oid_t, std::string> oid_t_to_string;
typedef tbb::concurrent_unordered_map<oid_t, storage::DataTable*> oid_t_to_datatable_ptr;

// database oid -> database address
static oid_t_to_database_ptr database_oid_to_address;

//===--------------------------------------------------------------------===//
// DataBase
//===--------------------------------------------------------------------===//

class Database{

public:
    Database(Database const&) = delete;  

    // Constructor
    Database( oid_t database_oid ) 
    : database_oid(database_oid) { }

    ~Database(){
      //drop all tables in current db
      DeleteAllTables();
    };

    //===--------------------------------------------------------------------===//
    // OPERATIONS
    //===--------------------------------------------------------------------===//
    
    // Create a new database 
    static Database* GetDatabaseById( oid_t database_oid );
    static bool DeleteDatabaseById( oid_t database_oid );

    bool AddTable( storage::DataTable* table );

    bool DeleteTableById( oid_t table_oid );
    bool DeleteTableByName( std::string table_name );
    bool DeleteAllTables();

    storage::DataTable* GetTableByName( const std::string table_name ) const;
    storage::DataTable* GetTableById( const oid_t table_oid ) const;
    storage::DataTable* GetTableByOffset( const oid_t table_offset ) const;

    oid_t GetTableIdByName( const std::string table_name ) const;
    std::string GetTableNameById( const oid_t table_oid ) const;


    inline size_t GetTableCount() const {
      return table_oid_to_address.size();
    }

    inline size_t GetDatabaseOid() const {
      return database_oid;
    }

    //===--------------------------------------------------------------------===//
    // UTILITIES
    //===--------------------------------------------------------------------===//

    // Get a string representation of this database
    friend std::ostream& operator<<(std::ostream& os, const Database& database);

protected:

    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//

    // database oid
    oid_t database_oid;

    // table name -> table oid
    string_to_oid_t table_name_to_oid;

    // table oid -> table name
    oid_t_to_string table_oid_to_name;

    // table oid -> table address
    oid_t_to_datatable_ptr table_oid_to_address;
};


} // End storage namespace
} // End peloton namespace


