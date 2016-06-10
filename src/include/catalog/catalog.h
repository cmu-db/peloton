//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.h
//
// Identification: src/include/catalog/catalog.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.h"
#include "catalog/schema.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {
class DataTable;
}

namespace catalog {

//===--------------------------------------------------------------------===//
// Bootstrap
//===--------------------------------------------------------------------===//

class Catalog {

public:

 // Global Singleton
 static Catalog &GetInstance(void);

 void AddDatabase(std::string database_name);

 storage::Database *GetDatabaseWithOid(const oid_t db_oid) const;

 storage::Database *GetDatabaseWithName(const std::string db_name) const;

 // Create Table for pg_class
 storage::DataTable *CreateTableCatalog(std::string table_name);

 // Create Table for pg_database
 storage::DataTable *CreateDatabaseCatalog(std::string table_name);

 std::unique_ptr<Schema> InitializeDatabaseSchema();
 std::unique_ptr<Schema> InitializeTablesSchema();

 oid_t GetDatabaseCount() const;

 oid_t GetNewID();

private:
 std::vector<storage::Database*> databases;
 oid_t id_cntr = START_OID;
 std::mutex catalog_mutex;



};
}
}
