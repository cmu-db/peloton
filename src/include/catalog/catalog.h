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
#include "storage/tuple.h"
#include "storage/table_factory.h"
#include "common/value_factory.h"
#include "catalog/catalog_util.h"

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
 static std::unique_ptr<Catalog> GetInstance(void);

 // Creates the catalog database
 void CreateCatalogDatabase(void);

 // Create a database
 void CreateDatabase(std::string database_name);

 // Create a table in a database
 void CreateTable(std::string database_name, std::string table_name, std::unique_ptr<catalog::Schema>);

 // Drop a table
 void DropTable(std::string database_name, std::string table_name);

 // Find a database using its id
 storage::Database *GetDatabaseWithOid(const oid_t db_oid) const;

 // Find a database using its name
 storage::Database *GetDatabaseWithName(const std::string db_name) const;

 // Create Table for pg_class
 std::unique_ptr<storage::DataTable> CreateTableCatalog(oid_t database_id, std::string table_name);

 // Create Table for pg_database
 std::unique_ptr<storage::DataTable> CreateDatabaseCatalog(oid_t database_id, std::string table_name);

 // Initialize the schema of the database catalog
 std::unique_ptr<Schema> InitializeDatabaseSchema();

 // Initialize the schema of the table catalog
 std::unique_ptr<Schema> InitializeTablesSchema();

 // Get the number of databases currently in the catalog
 int GetDatabaseCount();

 // Get a new id for database, table, etc.
 oid_t GetNewID();

private:
 // A vector of the database pointers in the catalog
 std::vector<storage::Database*> databases;

 // The id variable that get assigned to objects. Initialized with START_OID
 oid_t id_cntr = 1;

 // Mutex used for atomic operations
 std::mutex catalog_mutex;

 const size_t max_name_size = 32;



};
}
}
