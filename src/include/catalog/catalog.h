//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.h
//
// Identification: src/include/catalog/catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_util.h"
#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/schema.h"
#include "catalog/table_catalog.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "type/abstract_pool.h"
#include "type/catalog_type.h"
#include "type/ephemeral_pool.h"
#include "type/types.h"
#include "type/value_factory.h"

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

  // Create a database
  ResultType CreateDatabase(const std::string &database_name,
                            concurrency::Transaction *txn);

  // Add a database
  void AddDatabase(storage::Database *database);

  // Create a table in a database
  ResultType CreateTable(const std::string &database_name,
                         const std::string &table_name,
                         std::unique_ptr<catalog::Schema>,
                         concurrency::Transaction *txn);

  // Create the primary key index for a table
  ResultType CreatePrimaryIndex(const std::string &database_name,
                                const std::string &table_name);

  ResultType CreateIndex(const std::string &database_name,
                         const std::string &table_name,
                         std::vector<std::string> index_attr,
                         std::string index_name, bool unique,
                         IndexType index_type);

  // Get a index with the oids of index, table, and database.
  index::Index *GetIndexWithOid(oid_t database_oid, oid_t table_oid,
                                oid_t index_oid) const;
  // Drop a database
  ResultType DropDatabaseWithName(const std::string &database_name,
                                  concurrency::Transaction *txn);

  // Drop a database with its oid
  ResultType DropDatabaseWithOid(oid_t database_oid,
                                 concurrency::Transaction *txn);

  // Drop a table
  ResultType DropTable(const std::string &database_name, const std::string &table_name,
                       concurrency::Transaction *txn);
  // Drop a table, use this one in the future
  ResultType DropTable(oid_t database_oid, oid_t table_oid,
                       concurrency::Transaction *txn);
  // Drop an index, using its index_oid
  ResultType DropIndex(oid_t index_oid);

  // Returns true if the catalog contains the given database with the id
  bool HasDatabase(oid_t db_oid) const;

  // Find a database using its id
  // return nullptr if not found
  storage::Database *GetDatabaseWithOid(oid_t db_oid) const;

  // Find a database using its name
  // return nullptr if not found
  storage::Database *GetDatabaseWithName(const std::string &db_name) const;

  // Find a database using vector offset
  storage::Database *GetDatabaseWithOffset(oid_t database_offset) const;

  // Get table from a database with its name
  storage::DataTable *GetTableWithName(const std::string &database_name,
                                       const std::string &table_name);

  // Get table from a database with its oid
  storage::DataTable *GetTableWithOid(oid_t database_oid,
                                      oid_t table_oid) const;

  // Get the number of databases currently in the catalog
  oid_t GetDatabaseCount();

  //===--------------------------------------------------------------------===//
  // METRIC
  //===--------------------------------------------------------------------===//

  // Creates the metric tables for statistics
  void CreateMetricsCatalog();

  // Create Table for metrics tables
  std::unique_ptr<storage::DataTable> CreateMetricsCatalog(
      oid_t database_id, std::string table_name);

  // Initialize the schema of the database metrics table
  std::unique_ptr<Schema> InitializeDatabaseMetricsSchema();

  // Initialize the schema of the table metrics table
  std::unique_ptr<Schema> InitializeTableMetricsSchema();

  // Initialize the schema of the index metrics table
  std::unique_ptr<Schema> InitializeIndexMetricsSchema();

  // Initialize the schema of the query metrics table
  std::unique_ptr<catalog::Schema> InitializeQueryMetricsSchema();

  //===--------------------------------------------------------------------===//
  // FUNCTION
  //===--------------------------------------------------------------------===//

  void InitializeFunctions();

  void AddFunction(const std::string &name,
                   const std::vector<type::Type::TypeId> &argument_types,
                   const type::Type::TypeId return_type,
                   type::Value (*func_ptr)(const std::vector<type::Value> &));

  const FunctionData GetFunction(const std::string &name);

  void RemoveFunction(const std::string &name);

 private:
  Catalog();

  void InitializeCatalog(void);

  // Deconstruct the catalog database when destroying the catalog.
  ~Catalog();

  // A vector of the database pointers in the catalog
  std::vector<storage::Database *> databases_;

  // Map of function names to data about functions (number of arguments,
  // function ptr, return type)
  std::unordered_map<std::string, FunctionData> functions_;

  // The pool for new varlen tuple fields
  std::unique_ptr<type::AbstractPool> pool_ = new type::EphemeralPool();

  std::mutex catalog_mutex;
};
}
}
