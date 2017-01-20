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

#include "catalog/catalog_util.h"
#include "catalog/schema.h"
#include "type/types.h"
#include "type/value_factory.h"
#include "type/abstract_pool.h"
#include "type/ephemeral_pool.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"

#define CATALOG_DATABASE_NAME "catalog_db"
#define DATABASE_CATALOG_NAME "database_catalog"
#define TABLE_CATALOG_NAME "table_catalog"

#define DATABASE_METRIC_NAME "database_metric"
#define TABLE_METRIC_NAME "table_metric"
#define INDEX_METRIC_NAME "index_metric"
#define QUERY_METRIC_NAME "query_metric"

#define QUERY_NUM_PARAM_COL_NAME "num_params"
#define QUERY_PARAM_TYPE_COL_NAME "param_types"
#define QUERY_PARAM_FORMAT_COL_NAME "param_formats"
#define QUERY_PARAM_VAL_COL_NAME "param_values"

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {
class DataTable;
}

namespace catalog {

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//

// information about functions (for FunctionExpression)
struct FunctionData {
  // name of the function
  std::string func_name_;
  // type of input arguments
  std::vector<type::Type::TypeId> argument_types_;
  // funtion's return type
  type::Type::TypeId return_type_;
  // pointer to the funtion
  type::Value (*func_ptr_)(const std::vector<type::Value> &);
};

class Catalog {
 public:
  // Global Singleton
  static Catalog *GetInstance(void);

  Catalog();

  // Creates the catalog database
  void CreateCatalogDatabase(void);

  // Creates the metric tables for statistics
  void CreateMetricsCatalog();

  // Create a database
  ResultType CreateDatabase(std::string database_name,
                        concurrency::Transaction *txn);

  // Add a database
  void AddDatabase(storage::Database *database);

  // Add a database with name
  void AddDatabase(std::string database_name,
                   storage::Database *database);

  // Create a table in a database
  ResultType CreateTable(std::string database_name, std::string table_name,
                     std::unique_ptr<catalog::Schema>,
                     concurrency::Transaction *txn);

  // Create the primary key index for a table
  ResultType CreatePrimaryIndex(const std::string &database_name,
                            const std::string &table_name);

  ResultType CreateIndex(const std::string &database_name,
                     const std::string &table_name,
                     std::vector<std::string> index_attr,
                     std::string index_name, bool unique, IndexType index_type);

  // Get a index with the oids of index, table, and database.
  index::Index *GetIndexWithOid(const oid_t database_oid, const oid_t table_oid,
                                const oid_t index_oid) const;
  // Drop a database
  ResultType DropDatabaseWithName(std::string database_name,
                              concurrency::Transaction *txn);

  // Drop a database with its oid
  void DropDatabaseWithOid(const oid_t database_oid);

  // Drop a table
  ResultType DropTable(std::string database_name, std::string table_name,
                   concurrency::Transaction *txn);

  // Returns true if the catalog contains the given database with the id
  bool HasDatabase(const oid_t db_oid) const;

  // Find a database using its id
  // Throw CatalogException if not found
  storage::Database *GetDatabaseWithOid(const oid_t db_oid) const;

  // Find a database using its name
  // Throw CatalogException if not found
  storage::Database *GetDatabaseWithName(const std::string db_name) const;

  // Find a database using vector offset
  storage::Database *GetDatabaseWithOffset(const oid_t database_offset) const;

  // Create Table for pg_class
  std::unique_ptr<storage::DataTable> CreateTableCatalog(
      oid_t database_id, std::string table_name);

  // Create Table for pg_database
  std::unique_ptr<storage::DataTable> CreateDatabaseCatalog(
      oid_t database_id, std::string table_name);

  // Create Table for metrics tables
  std::unique_ptr<storage::DataTable> CreateMetricsCatalog(
      oid_t database_id, std::string table_name);

  // Initialize the schema of the database catalog
  std::unique_ptr<Schema> InitializeDatabaseSchema();

  // Initialize the schema of the table catalog
  std::unique_ptr<Schema> InitializeTablesSchema();

  // Initialize the schema of the database metrics table
  std::unique_ptr<Schema> InitializeDatabaseMetricsSchema();

  // Initialize the schema of the table metrics table
  std::unique_ptr<Schema> InitializeTableMetricsSchema();

  // Initialize the schema of the index metrics table
  std::unique_ptr<Schema> InitializeIndexMetricsSchema();

  // Initialize the schema of the query metrics table
  std::unique_ptr<catalog::Schema> InitializeQueryMetricsSchema();

  // Get table from a database with its name
  storage::DataTable *GetTableWithName(std::string database_name,
                                       std::string table_name);

  // Get table from a database with its oid
  storage::DataTable *GetTableWithOid(const oid_t database_oid,
                                      const oid_t table_oid) const;

  // Get the number of databases currently in the catalog
  oid_t GetDatabaseCount();

  void PrintCatalogs();

  // Get a new id for database, table, etc.
  oid_t GetNextOid();

  void InitializeFunctions();

  //===--------------------------------------------------------------------===//
  // FUNCTION ACCESS
  //===--------------------------------------------------------------------===//

  // add and get methods for functions
  void AddFunction(
      const std::string &name, const std::vector<type::Type::TypeId>& argument_types,
      const type::Type::TypeId return_type,
      type::Value (*func_ptr)(const std::vector<type::Value> &));

  const FunctionData GetFunction(const std::string &name);

  void RemoveFunction(const std::string &name);

  // Deconstruct the catalog database when destroy the catalog.
  ~Catalog();

 private:
  void InsertDatabaseIntoCatalogDatabase(oid_t database_id,
                                         std::string &database_name,
                                         concurrency::Transaction *txn);

  // A vector of the database pointers in the catalog
  std::vector<storage::Database *> databases_;

  // The id variable that get assigned to objects. Initialized with (START_OID
  // +
  // 1) because START_OID is assigned to the catalog database.
  std::atomic<oid_t> oid_ = ATOMIC_VAR_INIT(START_OID + 1);

  // Maximum Column Size for Catalog Schemas
  const size_t max_name_size = 32;

  // map of function names to data about functions (number of arguments,
  // function ptr, return type)
  std::unordered_map<std::string, FunctionData> functions_;

 public:

  // The pool for new varlen tuple fields
  type::AbstractPool *pool_ = new type::EphemeralPool();
};

}
}
