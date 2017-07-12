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

#include "catalog/catalog_defaults.h"
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
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"

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
  std::vector<type::TypeId> argument_types_;
  // funtion's return type
  type::TypeId return_type_;
  // pointer to the funtion
  type::Value (*func_ptr_)(const std::vector<type::Value> &);
};

class Catalog {
 public:
  // Global Singleton
  static Catalog *GetInstance(void);

  // Bootstrap addtional catalogs, only used in system initialization phase
  void Bootstrap(void);

  // Deconstruct the catalog database when destroying the catalog.
  ~Catalog();

  //===--------------------------------------------------------------------===//
  // CREATE FUNCTIONS
  //===--------------------------------------------------------------------===//
  // Create a database
  ResultType CreateDatabase(const std::string &database_name,
                            concurrency::Transaction *txn);

  // Create a table in a database
  ResultType CreateTable(const std::string &database_name,
                         const std::string &table_name,
                         std::unique_ptr<catalog::Schema>,
                         concurrency::Transaction *txn,
                         bool is_catalog = false);

  // Create the primary key index for a table, don't call this function outside
  // catalog.cpp
  ResultType CreatePrimaryIndex(oid_t database_oid, oid_t table_oid,
                                concurrency::Transaction *txn);
  // Create index for a table
  ResultType CreateIndex(const std::string &database_name,
                         const std::string &table_name,
                         const std::vector<std::string> &index_attr,
                         const std::string &index_name, bool unique_keys,
                         IndexType index_type, concurrency::Transaction *txn);

  ResultType CreateIndex(oid_t database_oid, oid_t table_oid,
                         const std::vector<std::string> &index_attr,
                         const std::string &index_name, IndexType index_type,
                         IndexConstraintType index_constraint, bool unique_keys,
                         concurrency::Transaction *txn,
                         bool is_catalog = false);

  //===--------------------------------------------------------------------===//
  // DROP FUNCTIONS
  //===--------------------------------------------------------------------===//
  // Drop a database with its name
  ResultType DropDatabaseWithName(const std::string &database_name,
                                  concurrency::Transaction *txn);

  // Drop a database with its oid
  ResultType DropDatabaseWithOid(oid_t database_oid,
                                 concurrency::Transaction *txn);

  // Drop a table using table name
  ResultType DropTable(const std::string &database_name,
                       const std::string &table_name,
                       concurrency::Transaction *txn);
  // Drop a table, use this one in the future
  ResultType DropTable(oid_t database_oid, oid_t table_oid,
                       concurrency::Transaction *txn);
  // Drop an index, using its index_oid
  ResultType DropIndex(oid_t index_oid,
                       concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // GET WITH NAME - CHECK FROM CATALOG TABLES, USING TRANSACTION
  //===--------------------------------------------------------------------===//

  /* Check database from pg_database with database_name using txn,
   * get it from storage layer using database_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  // FIXME: enforce caller not to use nullptr as txn
  storage::Database *GetDatabaseWithName(
      const std::string &db_name,
      concurrency::Transaction *txn = nullptr) const;

  /* Check table from pg_table with table_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  // FIXME: enforce caller not to use nullptr as txn
  storage::DataTable *GetTableWithName(const std::string &database_name,
                                       const std::string &table_name,
                                       concurrency::Transaction *txn = nullptr);
  //===--------------------------------------------------------------------===//
  // DEPRECATED FUNCTIONs
  //===--------------------------------------------------------------------===//
  /*
  * We're working right now to remove metadata from storage level and eliminate
  * multiple copies, so those functions below will be DEPRECATED soon.
  */

  // Add a database
  void AddDatabase(storage::Database *database);

  //===--------------------------------------------------------------------===//
  // USER DEFINE FUNCTION
  //===--------------------------------------------------------------------===//

  void InitializeFunctions();

  void AddFunction(const std::string &name,
                   const std::vector<type::TypeId> &argument_types,
                   const type::TypeId return_type,
                   type::Value (*func_ptr)(const std::vector<type::Value> &));

  const FunctionData GetFunction(const std::string &name);

  void RemoveFunction(const std::string &name);

 private:
  Catalog();

  // Map of function names to data about functions (number of arguments,
  // function ptr, return type)
  std::unordered_map<std::string, FunctionData> functions_;

  // The pool for new varlen tuple fields
  std::unique_ptr<type::AbstractPool> pool_;

  std::mutex catalog_mutex;
};
}
}
