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
    
    static Database* GetInstance(oid_t database_oid){
      Database* db_address;

      try {
        db_address = database_address_locator.at( database_oid );
      }catch (const std::out_of_range& oor) {
        Database* db = new Database( database_oid ); 
        db_address = db;
        database_address_locator.insert( std::pair<oid_t,Database*>( database_oid,
                                                                     db_address ));
      }
      return db_address;
    }

    // TODO :: relation id will be removed when we can get oid from catalog stably
    //         or store relation id in the table
    bool AddTable(storage::DataTable* table, oid_t relation_id){

      std::string table_name = table->GetName();
      oid_t table_oid = relation_id;

      try {
        table_oid = table_oid_locator.at( table_name );
        return false; // table already exists
      }catch (const std::out_of_range& oor) {
        table_oid_locator.insert( std::pair<std::string,oid_t>
                                           ( table_name, table_oid ));
      }
      table_address_locator.insert( std::pair<oid_t, storage::DataTable*>
                                             ( table_oid, table ));
      return true;
    }
    /*
    TODO ::
    inline storage::DataTable* GetTableByName(const std::string table_name) const{
      global_locator.insert(std::pair<std::pair<oid_t, oid_t>, void*>(db_and_table_oid_pair, location));
      table_name_to_oid
      //table_oid_locator.insert( std::pair<std::string,oid_t>( table_name, table_oid ))
      oid = table_oid_locator.at( table_name ); 
    }
    GetTableById(oid table_oid);
    GetTableCount()
    GetAllDataTable()
    DropAllDataTable()
   */

    //===--------------------------------------------------------------------===//
    // UTILITIES
    //===--------------------------------------------------------------------===//

    // Get a string representation of this table
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


