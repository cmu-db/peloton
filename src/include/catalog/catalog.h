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
class Constraint;
class Schema;
class DatabaseCatalogEntry;
class TableCatalogEntry;
class IndexCatalogEntry;
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
  ResultType CreateDatabase(concurrency::TransactionContext *txn,
                            const std::string &database_name);

  // Create a schema(namespace)
  ResultType CreateSchema(concurrency::TransactionContext *txn,
                          const std::string &database_name,
                          const std::string &schema_name);

  // Create a table in a database
  ResultType CreateTable(concurrency::TransactionContext *txn,
                         const std::string &database_name,
                         const std::string &schema_name,
                         std::unique_ptr<catalog::Schema> schema,
                         const std::string &table_name,
                         bool is_catalog,
                         uint32_t tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP,
                         LayoutType layout_type = LayoutType::ROW);

  // Create index for a table
  ResultType CreateIndex(concurrency::TransactionContext *txn,
                         const std::string &database_name,
                         const std::string &schema_name,
                         const std::string &table_name,
                         const std::string &index_name,
                         const std::vector<oid_t> &key_attrs,
                         bool unique_keys,
                         IndexType index_type);

  ResultType CreateIndex(concurrency::TransactionContext *txn,
                         oid_t database_oid,
                         const std::string &schema_name,
                         oid_t table_oid,
                         bool is_catalog,
                         oid_t index_oid,
                         const std::string &index_name,
                         const std::vector<oid_t> &key_attrs,
                         bool unique_keys,
                         IndexType index_type,
                         IndexConstraintType index_constraint);


  /**
   * @brief   create a new layout for a table
   * @param   database_oid  database to which the table belongs to
   * @param   table_oid     table to which the layout has to be added
   * @param   column_map    column_map of the new layout to be created
   * @param   txn           TransactionContext
   * @return  shared_ptr    shared_ptr to the newly created layout in case of
   *                        success. nullptr in case of failure.
   */
  std::shared_ptr<const storage::Layout> CreateLayout(concurrency::TransactionContext *txn,
                                                      oid_t database_oid,
                                                      oid_t table_oid,
                                                      const column_map_type &column_map);

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
  std::shared_ptr<const storage::Layout> CreateDefaultLayout(concurrency::TransactionContext *txn,
                                                             oid_t database_oid,
                                                             oid_t table_oid,
                                                             const column_map_type &column_map);

  //===--------------------------------------------------------------------===//
  // SET FUNCTIONS FOR COLUMN CONSTRAINT
  //===--------------------------------------------------------------------===//

  // Set not null constraint for a column
  ResultType SetNotNullConstraint(concurrency::TransactionContext *txn,
                                  oid_t database_oid,
                                  oid_t table_oid,
                                  oid_t column_id);

  // Set default constraint for a column
  ResultType SetDefaultConstraint(concurrency::TransactionContext *txn,
                                  oid_t database_oid,
                                  oid_t table_oid,
                                  oid_t column_id,
                                  const type::Value &default_value);

  //===--------------------------------------------------------------------===//
  // ADD FUNCTIONS FOR TABLE CONSTRAINT
  //===--------------------------------------------------------------------===//

  // Add a new primary constraint for a table
  ResultType AddPrimaryKeyConstraint(concurrency::TransactionContext *txn,
                                     oid_t database_oid,
                                     oid_t table_oid,
                                     const std::vector<oid_t> &column_ids,
                                     const std::string &constraint_name);

  // Add a new unique constraint for a table
  ResultType AddUniqueConstraint(concurrency::TransactionContext *txn,
                                 oid_t database_oid,
                                 oid_t table_oid,
                                 const std::vector<oid_t> &column_ids,
                                 const std::string &constraint_name);

  // Add a new foreign key constraint for a table
  ResultType AddForeignKeyConstraint(concurrency::TransactionContext *txn,
                                     oid_t database_oid,
                                     oid_t src_table_oid,
                                     const std::vector<oid_t> &src_col_ids,
                                     oid_t sink_table_oid,
                                     const std::vector<oid_t> &sink_col_ids,
                                     FKConstrActionType upd_action,
                                     FKConstrActionType del_action,
                                     const std::string &constraint_name);

  // Add a new check constraint for a table
  ResultType AddCheckConstraint(concurrency::TransactionContext *txn,
                                oid_t database_oid,
                                oid_t table_oid,
                                const std::vector<oid_t> &column_ids,
                                const std::pair<ExpressionType, type::Value> &exp,
                                const std::string &constraint_name);

  //===--------------------------------------------------------------------===//
  // DROP FUNCTIONS
  //===--------------------------------------------------------------------===//
  // Drop a database with its name
  ResultType DropDatabaseWithName(concurrency::TransactionContext *txn,
                                  const std::string &database_name);

  // Drop a database with its oid
  ResultType DropDatabaseWithOid(concurrency::TransactionContext *txn,
                                 oid_t database_oid);

  // Drop a schema(namespace) using schema name
  ResultType DropSchema(concurrency::TransactionContext *txn,
                        const std::string &database_name,
                        const std::string &schema_name);

  // Drop a table using table name
  ResultType DropTable(concurrency::TransactionContext *txn,
                       const std::string &database_name,
                       const std::string &schema_name,
                       const std::string &table_name);

  // Drop a table, use this one in the future
  ResultType DropTable(concurrency::TransactionContext *txn,
                       oid_t database_oid,
                       oid_t table_oid);

  // Drop an index, using its index_oid
  ResultType DropIndex(concurrency::TransactionContext *txn,
                       oid_t database_oid,
                       oid_t index_oid);

  /** @brief   Drop layout
   * tile_groups
   * @param   database_oid    the database to which the table belongs
   * @param   table_oid       the table to which the layout belongs
   * @param   layout_oid      the layout to be dropped
   * @param   txn             TransactionContext
   * @return  ResultType(SUCCESS or FAILURE)
   */
  ResultType DropLayout(concurrency::TransactionContext *txn,
                        oid_t database_oid,
                        oid_t table_oid,
                        oid_t layout_oid);

  // Drop not null constraint for a column
  ResultType DropNotNullConstraint(concurrency::TransactionContext *txn,
                                   oid_t database_oid,
                                   oid_t table_oid,
                                   oid_t column_id);

  // Drop default constraint for a column
  ResultType DropDefaultConstraint(concurrency::TransactionContext *txn,
                                   oid_t database_oid,
                                   oid_t table_oid,
                                   oid_t column_id);

  // Drop constraint for a table
  ResultType DropConstraint(concurrency::TransactionContext *txn,
                            oid_t database_oid,
                            oid_t table_oid,
                            oid_t constraint_oid);

  //===--------------------------------------------------------------------===//
  // GET WITH NAME - CHECK FROM CATALOG TABLES, USING TRANSACTION
  //===--------------------------------------------------------------------===//

  /* Check database from pg_database with database_name using txn,
   * get it from storage layer using database_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  storage::Database *GetDatabaseWithName(concurrency::TransactionContext *txn,
                                         const std::string &db_name) const;

  /* Check table from pg_table with table_name & schema_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  storage::DataTable *GetTableWithName(concurrency::TransactionContext *txn,
                                       const std::string &database_name,
                                       const std::string &schema_name,
                                       const std::string &table_name);

  /* Check table from pg_database with database_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseCatalogEntry(concurrency::TransactionContext *txn,
                                                                const std::string &database_name);

  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseCatalogEntry(concurrency::TransactionContext *txn,
                                                                oid_t database_oid);

  /* Check table from pg_table with table_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  std::shared_ptr<TableCatalogEntry> GetTableCatalogEntry(concurrency::TransactionContext *txn,
                                                          const std::string &database_name,
                                                          const std::string &schema_name,
                                                          const std::string &table_name);

  std::shared_ptr<TableCatalogEntry> GetTableCatalogEntry(concurrency::TransactionContext *txn,
                                                          oid_t database_oid,
                                                          oid_t table_oid);

  /*
   * Using database oid to get system catalog object
   */
  std::shared_ptr<SystemCatalogs> GetSystemCatalogs(oid_t database_oid);
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

  void AddProcedure(concurrency::TransactionContext *txn,
                    const std::string &name,
                    type::TypeId return_type,
                    const std::vector<type::TypeId> &argument_types,
                    oid_t prolang,
                    std::shared_ptr<peloton::codegen::CodeContext> code_context,
                    const std::string &func_src);

  // TODO(Tianyu): Somebody should comment on what the difference between name
  //               and func_name is. I am confused.
  void AddBuiltinFunction(concurrency::TransactionContext *txn,
                          const std::string &name,
                          function::BuiltInFuncType func,
                          const std::string &func_name,
                          type::TypeId return_type,
                          const std::vector<type::TypeId> &argument_types,
                          oid_t prolang);

  const FunctionData GetFunction(const std::string &name,
                                 const std::vector<type::TypeId> &argument_types);

 private:
  Catalog();

  void BootstrapSystemCatalogs(concurrency::TransactionContext *txn,
                               storage::Database *database);

  // The pool for new varlen tuple fields
  std::unique_ptr<type::AbstractPool> pool_;
  std::mutex catalog_mutex;
  // key: database oid
  // value: SystemCatalog object(including pg_table, pg_index and pg_attribute)
  std::unordered_map<oid_t, std::shared_ptr<SystemCatalogs>> catalog_map_;
};

}  // namespace catalog
}  // namespace peloton
