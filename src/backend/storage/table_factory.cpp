/*-------------------------------------------------------------------------
 *
 * table_factory.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/bridge.h"
#include "backend/storage/table_factory.h"

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/catalog/manager.h"
#include "backend/storage/data_table.h"

#include <mutex>

namespace nstore {
namespace storage {

DataTable* TableFactory::GetDataTable(oid_t database_id,
                                      catalog::Schema* schema,
                                      std::string table_name,
                                      size_t tuples_per_tilegroup_count) {
    // create a new backend
    // FIXME: We need a better way of managing these. Why not just embed it in
    //        directly inside of the table object?
    AbstractBackend* backend = new VMBackend();

    DataTable *table =  new DataTable(schema, backend, table_name,
                                      tuples_per_tilegroup_count);
    table->database_id = database_id;

    // Check if we need this table in the catalog
    if(database_id != INVALID_OID){
        unsigned int table_oid = GetRelationOidFromRelationName(table_name.c_str());
        tableMap[table_oid] = table; 
    }

    return table;

}

bool TableFactory::DropDataTable(std::string table_name){

    unsigned int table_oid = GetRelationOidFromRelationName(table_name.c_str());
    DataTable* table =  tableMap[table_oid];

    if( table == 0 ) 
        return false;
    delete table;
    
    return true;
}


} // End storage namespace
} // End nstore namespace

