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
class IndexCatalogObject;
class SystemCatalogs;
}  // namespace catalog

namespace codegen {
class CodeContext;
}  // namespace codegen

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

namespace index {
class Index;
}  // namespace index

namespace storage {
class Database;
class DataTable;
class Layout;
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
  /**
   * Global Singleton
   * @deprecated
   */
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

  // Create a schema(namespace)
  ResultType CreateSchema(const std::string &database_name,
                          const std::string &schema_name,
                          concurrency::TransactionContext *txn);

  // Create a table in a database
  ResultType CreateTable(
      const std::string &database_name, const std::string &schema_name,
      const std::string &table_name, std::unique_ptr<catalog::Schema>,
      concurrency::TransactionContext *txn, bool is_catalog = false,
      uint32_t tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP,
      peloton::LayoutType layout_type = LayoutType::ROW);

  // Create index for a table
  ResultType CreateIndex(const std::string &database_name,
                         const std::string &schema_name,
                         const std::string &table_name,
                         const std::vector<oid_t> &key_attrs,
                         const std::string &index_name, bool unique_keys,
                         IndexType index_type,
                         concurrency::TransactionContext *txn);

  ResultType CreateIndex(oid_t database_oid, oid_t table_oid,
                         const std::vector<oid_t> &key_attrs,
                         const std::string &schema_name,
                         const std::string &index_name, IndexType index_type,
                         IndexConstraintType index_constraint, bool unique_keys,
                         concurrency::TransactionContext *txn,
                         bool is_catalog = false);

  /**
   * @brief   create a new layout for a table
   * @param   database_oid  database to which the table belongs to
   * @param   table_oid     table to which the layout has to be added
   * @param   column_map    column_map of the new layout to be created
   * @param   txn           TransactionContext
   * @return  shared_ptr    shared_ptr to the newly created layout in case of
   *                        success. nullptr in case of failure.
   */
  std::shared_ptr<const storage::Layout> CreateLayout(
      oid_t database_oid, oid_t table_oid, const column_map_type &column_map,
      concurrency::TransactionContext *txn);

  /**
   * @brief   create a new layout for a table and make it the default if
   *          if the creating is successsful.
   * @param   database_oid  database to which the table belongs to
   * @param   table_oid     table to which the layout has to be added
   * @param   column_map    column_map of the new layout to be created
   * @param   txn           TransactionContext
   * @return  shared_ptr    shared_ptr to the newly created layout in case of
   *                        success. nullptr in case of failure.
   */
  std::shared_ptr<const storage::Layout> CreateDefaultLayout(
      oid_t database_oid, oid_t table_oid, const column_map_type &column_map,
      concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // DROP FUNCTIONS
  //===--------------------------------------------------------------------===//
  // Drop a database with its name
  ResultType DropDatabaseWithName(const std::string &database_name,
                                  concurrency::TransactionContext *txn);

  // Drop a database with its oid
  ResultType DropDatabaseWithOid(oid_t database_oid,
                                 concurrency::TransactionContext *txn);

  // Drop a schema(namespace) using schema name
  ResultType DropSchema(const std::string &database_name,
                        const std::string &schema_name,
                        concurrency::TransactionContext *txn);

  // Drop a table using table name
  ResultType DropTable(const std::string &database_name,
                       const std::string &schema_name,
                       const std::string &table_name,
                       concurrency::TransactionContext *txn);
  // Drop a table, use this one in the future
  ResultType DropTable(oid_t database_oid, oid_t table_oid,
                       concurrency::TransactionContext *txn);
  // Drop an index, using its index_oid
  ResultType DropIndex(oid_t database_oid, oid_t index_oid,
                       concurrency::TransactionContext *txn);

  /** @brief   Drop layout
   * tile_groups
   * @param   database_oid    the database to which the table belongs
   * @param   table_oid       the table to which the layout belongs
   * @param   layout_oid      the layout to be dropped
   * @param   txn             TransactionContext
   * @return  ResultType(SUCCESS or FAILURE)
   */
  ResultType DropLayout(oid_t database_oid, oid_t table_oid, oid_t layout_oid,
                        concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // GET WITH NAME - CHECK FROM CATALOG TABLES, USING TRANSACTION
  //===--------------------------------------------------------------------===//

  /**
   * Check database from pg_database with database_name using txn,
   * get it from storage layer using database_oid,
   * throw exception and abort txn if not exists/invisible
   */
  storage::Database *GetDatabaseWithName(
      const std::string &db_name, concurrency::TransactionContext *txn) const;


  /**
   * Get the database catalog object from pg_database
   * throw exception and abort txn if not exists/invisible
   * @param database_name
   * @param txn
   * @return
   */
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      const std::string &database_name, concurrency::TransactionContext *txn);

  /**
   *
   * @param database_oid
   * @param txn
   * @return
   */
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      oid_t database_oid, concurrency::TransactionContext *txn);

 /**
  * Check table from pg_table with table_name & schema_name using txn,
  * get it from storage layer using table_oid,
  * throw exception and abort txn if not exists/invisible
  * @param database_name
  * @param schema_name
  * @param table_name
  * @param txn
  * @return
  */
  storage::DataTable *GetTableWithName(const std::string &database_name,
                                       const std::string &schema_name,
                                       const std::string &table_name,
                                       concurrency::TransactionContext *txn);

  /**
   * Check table from pg_table with table_name & schema_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * @param database_name
   * @param schema_name
   * @param table_name
   * @param txn
   * @return
   */
  std::shared_ptr<TableCatalogObject> GetTableObject(
      const std::string &database_name, const std::string &schema_name,
      const std::string &table_name, concurrency::TransactionContext *txn);

  /**
   * Check table from pg_table with table_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * @param database_oid
   * @param table_oid
   * @param txn
   * @return
   */
  std::shared_ptr<TableCatalogObject> GetTableObject(
      oid_t database_oid, oid_t table_oid,
      concurrency::TransactionContext *txn);

  /**
   * @brief Using database oid to get system catalog object
   * @param database_oid
   * @return
   */
  std::shared_ptr<SystemCatalogs> GetSystemCatalogs(const oid_t database_oid);

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

  /**
   * Add a new built-in function. This proceeds in two steps:
   *   1. Add the function information into pg_catalog.pg_proc
   *   2. Register the function pointer in function::BuiltinFunction
   * @param name function name used in SQL
   * @param argument_types arg types used in SQL
   * @param return_type the return type
   * @param prolang the oid of which language the function is
   * @param func_name the function name in C++ source (should be unique)
   * @param func the pointer to the function
   * @param txn
   */
  void AddBuiltinFunction(const std::string &name,
                          const std::vector<type::TypeId> &argument_types,
                          const type::TypeId return_type, oid_t prolang,
                          const std::string &func_name,
                          function::BuiltInFuncType func,
                          concurrency::TransactionContext *txn);

  const FunctionData GetFunction(
      const std::string &name, const std::vector<type::TypeId> &argument_types);

 private:

  /**
   * Initialization of catalog, including:
   *    (1) create peloton database, create catalog tables, add them into
   *        peloton database, insert columns into pg_attribute
   *    (2) create necessary indexes, insert into pg_index
   *    (3) insert peloton into pg_database, catalog tables into pg_table
   */
  Catalog();

  /**
   * This function *MUST* be called after a new database is created to
   * bootstrap all system catalog tables for that database. The system catalog
   * tables must be created in certain order to make sure all tuples are indexed
   *
   * @param   database    database which this system catalogs belong to
   * @param   txn         transaction context
   */
  void BootstrapSystemCatalogs(storage::Database *database,
                               concurrency::TransactionContext *txn);

  // Create the primary key index for a table, don't call this function outside
  // catalog.cpp
  ResultType CreatePrimaryIndex(oid_t database_oid, oid_t table_oid,
                                const std::string &schema_name,
                                concurrency::TransactionContext *txn);

  // The pool for new varlen tuple fields
  std::unique_ptr<type::AbstractPool> pool_;
  std::mutex catalog_mutex;
  // key: database oid
  // value: SystemCatalog object(including pg_table, pg_index and pg_attribute)
  std::unordered_map<oid_t, std::shared_ptr<SystemCatalogs>> catalog_map_;
};

}  // namespace catalog
}  // namespace peloton
