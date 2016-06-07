//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column.cpp
//
// Identification: src/catalog/bootstrap.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/bootstrap.h"

namespace peloton {
namespace catalog {


// Bootstrap Catalog
void bootstrap() {

	InitializeCatalogsSchemas();

}

// Initialize Schemas
void InitializeCatalogsSchemas() {
InitializeTablesSchema();
InitializeDatabaseSchema();
}

// Initialize tables catalog schema
void InitializeTablesSchema() {
	const bool is_inlined = true;
		const std::string not_null_constraint_name = "not_null";

		auto id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
		                          "table_id", is_inlined);
		id_column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
											   not_null_constraint_name));
		auto name_column = catalog::Column(VALUE_TYPE_VARCHAR, GetTypeSize(VALUE_TYPE_VARCHAR),
				                          "table_name", is_inlined);
		name_column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
													   not_null_constraint_name));
		Bootstrap::table_schema = new catalog::Schema({id_column, name_column});
}

// Initialize database catalog schema
void InitializeDatabaseSchema() {
	const bool is_inlined = true;
		const std::string not_null_constraint_name = "not_null";

		auto id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
		                          "database_id", is_inlined);
		id_column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
											   not_null_constraint_name));
		auto name_column = catalog::Column(VALUE_TYPE_VARCHAR, GetTypeSize(VALUE_TYPE_VARCHAR),
				                          "database_name", is_inlined);
		name_column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
													   not_null_constraint_name));
		Bootstrap::database_schema = new catalog::Schema({id_column, name_column});
}

// Create Table for pg_class
storage::DataTable CreateTableCatalog(oid_t table_oid, std::string table_name) {
	bool own_schema = true;
	bool adapt_table = false;

	storage::DataTable *table = storage::TableFactory::GetDataTable(
			  INVALID_OID, table_oid, Bootstrap::table_schema, table_name,
			  DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);
	return table;
}

// Create Table for pg_database
storage::DataTable CreateDatabaseCatalog(oid_t table_oid, std::string table_name) {
	bool own_schema = true;
	bool adapt_table = false;

	storage::DataTable *table = storage::TableFactory::GetDataTable(
			  INVALID_OID, table_oid, Bootstrap::database_schema, table_name,
			  DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);
	return table;
}


}
}
