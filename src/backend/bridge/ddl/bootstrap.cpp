//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bootstrap.cpp
//
// Identification: src/backend/bridge/ddl/bootstrap.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "backend/bridge/ddl/bootstrap.h"
#include "backend/storage/database.h"
#include "backend/common/logger.h"

namespace peloton {
namespace bridge {

/**
 * @brief This function constructs all the user-defined tables and indices in
 * all databases
 * @return true or false, depending on whether we could bootstrap.
 */
bool Bootstrap::BootstrapPeloton(void) {

  raw_database_info raw_database(MyDatabaseId);

  raw_database.CollectRawTableAndIndex();
  raw_database.CollectRawForeignKeys();

  // create the database with current database id
  elog(LOG, "Initializing database %s(%u) in Peloton",
       raw_database.GetDbName().c_str(), raw_database.GetDbOid());

  bool status = raw_database.CreateDatabase();

  // skip if we already initialize current database
  if (status == false) return false;

  // Create objects in Peloton
  raw_database.CreateTables();
  raw_database.CreateIndexes();
  raw_database.CreateForeignkeys();

  //auto &manager = catalog::Manager::GetInstance();
  // TODO: Update stats
  //auto db = manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());
  //db->UpdateStats(peloton_status, false);

  // Verbose mode
  //std::cout << "Print db :: \n"<<*db << std::endl;

  elog(LOG, "Finished initializing Peloton");
  return true;
}

}  // namespace bridge
}  // namespace peloton
