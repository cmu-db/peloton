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

#include <mutex>

#include "catalog/catalog_defaults.h"
#include "function/functions.h"

namespace peloton {

namespace catalog {
class Schema;
class DatabaseCatalogObject;
class TableCatalogObject;
}  // namespace catalog

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

namespace index {
class Index;
}  // namespace index

namespace storage {
class Database;
class DataTable;
class TableFactory;
class Tuple;
}  // namespace storage

namespace type {
class AbstractPool;
class Value;
class ValueFactory;
class Value;
}  // namespace type

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
  // function's return type
  type::TypeId return_type_;
  // indicates if PL/pgSQL udf
  bool is_udf_;
  // pointer to the function code_context (populated if UDF)
  std::shared_ptr<peloton::codegen::CodeContext> func_context_;
  // pointer to the function (populated if built-in)
  function::BuiltInFuncType func_;
};

class Catalog {
 public:
  // Global Singleton
  static Catalog *GetInstance();

  // Bootstrap additional catalogs, only used in system initialization phase
  void Bootstrap();

  // Deconstruct the catalog database when destroying the catalog.
  ~Catalog();

  //===--------------------------------------------------------------------===//
  // CREATE FUNCTIONS
  //===--------------------------------------------------------------------===//
  // Create a database
  ResultType CreateDatabase(const std::string &database_name,
                            concurrency::TransactionContext *txn);

  // Create a table in a database
  ResultType CreateTable(
      const std::string &database_name, const std::string &table_name,
      std::unique_ptr<catalog::Schema>, concurrency::TransactionContext *txn,
      bool is_catalog = false,
      oid_t tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP);

  // Create the primary key index for a table, don't call this function outside
  // catalog.cpp
  ResultType CreatePrimaryIndex(oid_t database_oid, oid_t table_oid,
                                concurrency::TransactionContext *txn);
  // Create index for a table
  ResultType CreateIndex(const std::string &database_name,
                         const std::string &table_name,
                         const std::vector<oid_t> &key_attrs,
                         const std::string &index_name, bool unique_keys,
                         IndexType index_type,
                         concurrency::TransactionContext *txn);

  ResultType CreateIndex(oid_t database_oid, oid_t table_oid,
                         const std::vector<oid_t> &key_attrs,
                         const std::string &index_name, IndexType index_type,
                         IndexConstraintType index_constraint, bool unique_keys,
                         concurrency::TransactionContext *txn,
                         bool is_catalog = false);

  //===--------------------------------------------------------------------===//
  // DROP FUNCTIONS
  //===--------------------------------------------------------------------===//
  // Drop a database with its name
  ResultType DropDatabaseWithName(const std::string &database_name,
                                  concurrency::TransactionContext *txn);

  // Drop a database with its oid
  ResultType DropDatabaseWithOid(oid_t database_oid,
                                 concurrency::TransactionContext *txn);

  // Drop a table using table name
  ResultType DropTable(const std::string &database_name,
                       const std::string &table_name,
                       concurrency::TransactionContext *txn);
  // Drop a table, use this one in the future
  ResultType DropTable(oid_t database_oid, oid_t table_oid,
                       concurrency::TransactionContext *txn);
  // Drop an index, using its index_oid
  ResultType DropIndex(oid_t index_oid, concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // GET WITH NAME - CHECK FROM CATALOG TABLES, USING TRANSACTION
  //===--------------------------------------------------------------------===//

  /* Check database from pg_database with database_name using txn,
   * get it from storage layer using database_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  storage::Database *GetDatabaseWithName(
      const std::string &db_name, concurrency::TransactionContext *txn) const;

  /* Check table from pg_table with table_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  storage::DataTable *GetTableWithName(const std::string &database_name,
                                       const std::string &table_name,
                                       concurrency::TransactionContext *txn);

  /* Check table from pg_database with database_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      const std::string &database_name, concurrency::TransactionContext *txn);
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      oid_t database_oid, concurrency::TransactionContext *txn);

  /* Check table from pg_table with table_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  std::shared_ptr<TableCatalogObject> GetTableObject(
      const std::string &database_name, const std::string &table_name,
      concurrency::TransactionContext *txn);
  std::shared_ptr<TableCatalogObject> GetTableObject(
      oid_t database_oid, oid_t table_oid,
      concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // DEPRECATED FUNCTIONS
  //===--------------------------------------------------------------------===//
  /*
   * We're working right now to remove metadata from storage level and eliminate
   * multiple copies, so those functions below will be DEPRECATED soon.
   */

  // Add a database
  void AddDatabase(storage::Database *database);

  //===--------------------------------------------------------------------===//
  // BUILTIN FUNCTION
  //===--------------------------------------------------------------------===//

  void InitializeLanguages();

  void InitializeFunctions();

  void AddPlpgsqlFunction(
      const std::string &name, const std::vector<type::TypeId> &argument_types,
      const type::TypeId return_type, oid_t prolang,
      const std::string &func_src,
      std::shared_ptr<peloton::codegen::CodeContext> code_context,
      concurrency::TransactionContext *txn);

  void AddBuiltinFunction(const std::string &name,
                          const std::vector<type::TypeId> &argument_types,
                          const type::TypeId return_type, oid_t prolang,
                          const std::string &func_name,
                          function::BuiltInFuncType func,
                          concurrency::TransactionContext *txn);

  const FunctionData GetFunction(
      const std::string &name, const std::vector<type::TypeId> &argument_types);

 private:
  Catalog();

  // The pool for new varlen tuple fields
  std::unique_ptr<type::AbstractPool> pool_;

  std::mutex catalog_mutex;
};

}  // namespace catalog
}  // namespace peloton
