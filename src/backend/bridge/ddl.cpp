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

int DDL::CreateTable(char* table_name, int arg){
    // FIXME: Get default db 
    catalog::Database* db = catalog::Catalog::GetInstance().GetDatabase(DEFAULT_DB_NAME);
    assert(db);

    executor::CreateExecutor::CreateTable(db, table_name, NULL ); 
    return arg * 2;
}

extern "C" int DDL_CreateTable(char* table_name, int arg) {
    return DDL::CreateTable(table_name, arg);
}

} // namespace bridge
} // namespace nstore
