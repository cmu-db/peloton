//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bootstrap.h
//
// Identification: src/include/catalog/bootstrap.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.h"

namespace peloton {

namespace catalog{
class Schema;
}

namespace storage{
class DataTable;
}

namespace catalog {

//===--------------------------------------------------------------------===//
// Bootstrap
//===--------------------------------------------------------------------===//

class Bootstrap{

public:

	// Bootstrap Catalog
	void bootstrap();

	// Initialize Schemas
	void InitializeCatalogsSchemas();

	// Create Table for pg_class
	storage::DataTable CreateTableCatalog(oid_t table_oid, std::string table_name, catalog::Schema *table_schema);

	// Create Table for pg_database
	storage::DataTable CreateDatabaseCatalog(oid_t table_oid, std::string table_name, catalog::Schema *table_schema);

	// TODO: Other Catalogs should be added here

};

}
}
