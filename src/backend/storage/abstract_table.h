/*-------------------------------------------------------------------------
 *
 * abstract_table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"

#include <string>

namespace peloton {
namespace storage {

/**
 * Base class for all tables
 */
class AbstractTable {
 public:
  virtual ~AbstractTable();

 protected:
  // Table constructor
  AbstractTable(oid_t table_oid, std::string table_name,
                catalog::Schema* schema);

 public:
  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  std::string GetName() const { return table_name; }

  oid_t GetOid() const { return table_oid; }

  void SetSchema(catalog::Schema* given_schema) { schema = given_schema; }

  const catalog::Schema* GetSchema() const { return schema; }

  catalog::Schema* GetSchema() { return schema; }

  // Get a string representation of this table
  friend std::ostream& operator<<(std::ostream& os, const AbstractTable& table);

 protected:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Catalog information
  oid_t table_oid;
  oid_t database_oid;

  // table name
  std::string table_name;

  // table schema
  catalog::Schema* schema;
};

}  // End storage namespace
}  // End peloton namespace
