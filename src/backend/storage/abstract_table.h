//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_table.h
//
// Identification: src/backend/storage/abstract_table.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
  AbstractTable(oid_t database_oid, oid_t table_oid, std::string table_name,
                catalog::Schema *schema, bool own_schema);

 public:
  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  std::string GetName() const {
    return table_name;
  }

  oid_t GetOid() const {
    return table_oid;
  }

  oid_t GetDatabaseOid() const {
    return database_oid;
  }

  void SetSchema(catalog::Schema *given_schema) {
    schema = given_schema;
  }

  const catalog::Schema *GetSchema() const {
    return schema;
  }

  catalog::Schema *GetSchema() {
    return schema;
  }

  // Get a string representation of this table
  friend std::ostream &operator<<(std::ostream &os, const AbstractTable &table);

 protected:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Catalog information
  oid_t database_oid;

  oid_t table_oid;

  // table name
  std::string table_name;

  // table schema
  catalog::Schema *schema;

  /**
   * @brief Should this table own the schema?
   * Usually true.
   * Will be false if the table is for intermediate results within a query,
   * where the scheme may live longer.
   */
  bool own_schema_;
};

}  // End storage namespace
}  // End peloton namespace
