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
#include <cassert>

#include "backend/catalog/catalog_object.h"
#include "backend/catalog/schema.h"
#include "backend/catalog/index.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace index{
class Index;
}

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
                 parent,
                 root),
                 table_name(table_name){
  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  std::string GetName() const {
    return table_name;
  }

  storage::DataTable *GetTable() const {
    return data_table_;
  }

  void SetDataTable(storage::DataTable* table) {
    data_table_ = table;
  }

  storage::DataTable *GetDataTable() {
    return data_table_;
  }

  void SetSchema(catalog::Schema *schema) {
    assert(GetChildrenCount(schema_collection) == 0);
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      AddChild(schema_collection, static_cast<CatalogObject*>(schema));
    }
  }

  catalog::Schema *GetSchema() {
    catalog::Schema *schema = nullptr;
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      schema = static_cast<catalog::Schema *>(GetChildWithID(schema_collection, 0));
    }
    return schema;
  }

  void AddIndex(catalog::Index *catalog) {
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      AddChild(index_collection, static_cast<CatalogObject*>(catalog));
    }
  }

  catalog::Index *GetIndex(const oid_t index_id) {
    catalog::Index *index = nullptr;
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      index = static_cast<catalog::Index *>(GetChildWithID(index_collection, index_id));
    }
    return index;
  }

  void DropIndex(const oid_t index_id) {
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      DropChildWithID(index_collection, index_id);
    }
  }

  // Get a string representation of this table
  friend std::ostream& operator<<(std::ostream& os, const Table& table);

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // underlying physical table
  storage::DataTable *data_table_ = nullptr;

  // table name
  std::string table_name;

  // schema collection
  constexpr static oid_t schema_collection = 0;

  // index collection
  constexpr static oid_t index_collection = 1;

  // table mutex
  std::mutex table_mutex;

};

} // End catalog namespace
} // End peloton namespace

