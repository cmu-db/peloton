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
    AddChild(schema_collection, static_cast<CatalogObject*>(schema));
  }

  void AddIndex(catalog::Index *index) {
    AddChild(index_collection, static_cast<CatalogObject*>(index));
  }

  catalog::Schema *GetSchema() {
    return static_cast<catalog::Schema *>(GetChild(schema_collection, 0));
  }

  catalog::Index *GetIndex(const oid_t index_offset) {
    return static_cast<catalog::Index *>(GetChild(index_collection, index_offset));
  }

  std::string GetName() const {
    return table_name;
  }

  void Lock() {
    table_mutex.lock();
  }

  void Unlock() {
    table_mutex.unlock();
  }

  // Get a string representation of this table
  friend std::ostream& operator<<(std::ostream& os, const Table& table);

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // underlying physical table
  storage::DataTable *data_table_ = nullptr;

  // schema child offset
  constexpr static oid_t schema_collection = 0;

  // indices child offset
  constexpr static oid_t index_collection = 1;

  std::string table_name;

  std::mutex table_mutex;

};

} // End catalog namespace
} // End peloton namespace

