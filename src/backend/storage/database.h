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
// database oid -> database address
typedef tbb::concurrent_unordered_map<oid_t, Database*> database_oid_to_address;
// table name -> table oid
typedef tbb::concurrent_unordered_map<std::string, oid_t> table_name_to_oid;
// table oid -> table address
typedef tbb::concurrent_unordered_map<oid_t, storage::DataTable*> table_oid_to_address;

static database_oid_to_address database_address_locator;

//===--------------------------------------------------------------------===//
// DataBase
//===--------------------------------------------------------------------===//

class Database{

public:
    Database(Database const&) = delete;  

    // Constructor
    Database( oid_t database_oid ) : database_oid(database_oid){}

    ~Database()
    {
      //drop all tables in current db
    };

    //===--------------------------------------------------------------------===//
    // OPERATIONS
    //===--------------------------------------------------------------------===//
    
    static Database* GetDatabaseById(oid_t database_oid);

    bool AddTable(storage::DataTable* table);

    storage::DataTable* GetTableByName(const std::string table_name) const;
    storage::DataTable* GetTableById(const oid_t table_oid) const;
    storage::DataTable* GetTableByPosition(const oid_t table_position) const;

    inline size_t GetTableCount() const{
      return table_address_locator.size();  
    }

    inline size_t GetDatabaseOid() const{
      return database_oid;
    }

    /*
    TODO :: 
    GetAllTableList()
    DeleteTableByName()
    DeleteTableById()
    DropAllDataTable()
    */

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

    // map from table name to table oid
    table_name_to_oid table_oid_locator;
    // map from table oid to table address
    table_oid_to_address table_address_locator;
};


} // End storage namespace
} // End peloton namespace


