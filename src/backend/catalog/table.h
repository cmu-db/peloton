/*-------------------------------------------------------------------------
 *
 * table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/table.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <iostream>
#include <algorithm>

#include "backend/catalog/catalog_object.h"
#include "backend/catalog/constraint.h"
#include "backend/catalog/column.h"
#include "backend/catalog/index.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Table
//===--------------------------------------------------------------------===//

class Table : public CatalogObject {

 public:

  Table(oid_t table_oid,
        std::string table_name,
        CatalogObject *parent,
        CatalogObject *root)
 : CatalogObject(table_oid,
                 table_name,
                 parent,
                 root) {
  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  storage::DataTable *GetTable() const {
    return data_table_;
  }

  void SetDataTable(storage::DataTable* table) {
    data_table_ = table;
  }

  bool SetSchema(catalog::Schema *schema){
    return CreateChild(schema_child_oid, static_cast<CatalogObject*>(schema));
  }

  storage::DataTable *GetDataTable() {
    return data_table_;
  }

  catalog::Schema *GetSchema() const {
    return GetChild(schema_child_oid);
  }

  // Get a string representation of this table
  friend std::ostream& operator<<(std::ostream& os, const Table& table);

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // underlying physical table
  storage::DataTable *data_table_ = nullptr;

  // schema oid
  constexpr static oid_t schema_child_oid = 1;

};

} // End catalog namespace
} // End peloton namespace

