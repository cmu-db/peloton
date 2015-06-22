/*-------------------------------------------------------------------------
 *
 * dll.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/bridge/ddl.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/ddl.h"
#include "backend/executor/create_executor.h"

#include <cassert>

namespace nstore {
namespace bridge {

int DDL::CreateTable(char* table_name, DDL_Column* columns, int number_of_columns){
    int ret = 0;
    // FIXME: Get default db 
    catalog::Database* db = catalog::Catalog::GetInstance().GetDatabase(DEFAULT_DB_NAME);
    assert(db);
    ret = executor::CreateExecutor::CreateTable(db, table_name, columns, number_of_columns, NULL ); 

    return  ret;
}

extern "C" int DDL_CreateTable(char* table_name, DDL_Column* columns, int number_of_columns) {
    return DDL::CreateTable(table_name, columns, number_of_columns);
}

} // namespace bridge
} // namespace nstore
