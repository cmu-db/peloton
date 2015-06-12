/*-------------------------------------------------------------------------
 *
 * table_factory.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "storage/table_factory.h"

#include "common/exception.h"
#include "index/index.h"
#include "common/logger.h"
#include "catalog/manager.h"

#include <mutex>

namespace nstore {
namespace storage {

DataTable* TableFactory::GetDataTable(oid_t database_id, catalog::Schema* schema) {
    // create a new backend
    // FIXME: We need a better way of managing these. Why not just embed it in
    //        directly inside of the table object?
    Backend* backend = new VMBackend();

    DataTable *table =  new DataTable(schema, backend, table_name);
    table->database_id = database_id; // FIXME

    return table;

}


} // End storage namespace
} // End nstore namespace

