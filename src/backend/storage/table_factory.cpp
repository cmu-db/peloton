/*-------------------------------------------------------------------------
 *
 * table_factory.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/storage/table_factory.h"

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/catalog/manager.h"
#include "backend/storage/data_table.h"

#include "bridge/bridge.h"

#include <mutex>

namespace peloton {
namespace storage {

DataTable* TableFactory::GetDataTable(oid_t database_id,
                                      const catalog::Schema* schema,
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
        //oid_t table_oid = GetRelationOid(table_name.c_str());
        catalog::Manager::GetInstance().SetLocation(database_id, table_name, table);
    }

    return table;

}

bool TableFactory::DropDataTable(oid_t database_oid, oid_t table_oid)
{

    DataTable* table = (DataTable*) catalog::Manager::GetInstance().GetLocation(database_oid, table_oid);

    if(table == nullptr)
      return false;

    delete table;
    return true;
}
bool TableFactory::DropDataTable(oid_t database_oid, std::string table_name)
{

    DataTable* table = (DataTable*) catalog::Manager::GetInstance().GetLocation(database_oid, table_name);

    if(table == nullptr)
      return false;

    delete table;
    return true;
}


} // End storage namespace
} // End peloton namespace

