//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog.h
//
// Identification: src/include/catalog_objects.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include "catalog/abstract_catalog.h"
#include "catalog/schema.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/printable.h"
#include "type/abstract_pool.h"
#include "type/types.h"
#include "type/value.h"

namespace peloton {
namespace catalog {

/////////////////////////////////////////////////////////////////////
// TableCatalogObject class definition
/////////////////////////////////////////////////////////////////////

/*
 * class TableCatalogObject - Holds metadata of an table object
 *
 * The TableCatalog object maintains the database oid, table oid,
 * table name and corresponding tuple schema.
 */

class TableCatalogObject : public AbstractCatalogObject {
  // Don't allow to call default constructor
  TableCatalogObject() = delete;

 public:
  TableCatalogObject(std::string table_name, oid_t table_oid,
                     oid_t database_oid, Schema *tuple_schema, bool own_schema)
      : AbstractCatalogObject(table_name, table_oid),
        database_oid(database_oid),
        schema_(tuple_schema),
      	own_schema_(own_schema) {}

  ~TableCatalogObject();

  inline oid_t GetDatabaseOid() { return database_oid; }

  inline Schema *GetSchema() { return schema_; }

  /*
   * GetInfo() - Get a string representation for debugging
   */
  const std::string GetInfo() const;

  ///////////////////////////////////////////////////////////////////
  // IndexCatalog Data Member Definition
  ///////////////////////////////////////////////////////////////////
 private:
  oid_t database_oid;

  // schema of the table
  Schema *schema_;
  bool own_schema_;
};


/*
 * class DatabaseCatalogObject - Holds metadata of an table object
 *
 * The DatabaseCatalog object maintains the database oid, table oid,
 * table name and corresponding tuple schema.
 */

class DatabaseCatalogObject : public AbstractCatalogObject {
  // Don't allow to call default constructor
  DatabaseCatalogObject() = delete;

 public:
  DatabaseCatalogObject(std::string database_name, oid_t database_oid)
      : AbstractCatalogObject(database_name, database_oid) {}

  ~DatabaseCatalogObject();

  /*
   * GetInfo() - Get a string representation for debugging
   */
  const std::string GetInfo() const;
};
}  // end of namespace catalog
}  // end of namespace Peloton