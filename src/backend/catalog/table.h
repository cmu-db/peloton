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

  //===--------------------------------------------------------------------===//
  // SCHEMA
  //===--------------------------------------------------------------------===//

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

  //===--------------------------------------------------------------------===//
  // INDEX
  //===--------------------------------------------------------------------===//

  void AddIndex(catalog::Index *index) {
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      AddChild(index_collection, static_cast<CatalogObject*>(index));
    }

    // Update index stats
    auto index_type = index->GetPhysicalIndex()->GetIndexType();
    if(index_type == INDEX_TYPE_PRIMARY_KEY) {
      has_primary_key = true;
    }
    else if(index_type == INDEX_TYPE_UNIQUE) {
      unique_constraint_count++;
    }

  }

  catalog::Index *GetIndexWithID(const oid_t index_id) {
    catalog::Index *index = nullptr;
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      index = static_cast<catalog::Index *>(GetChildWithID(index_collection, index_id));
    }
    return index;
  }

  catalog::Index *GetIndex(const oid_t index_offset) {
    catalog::Index *index = nullptr;
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      index = static_cast<catalog::Index *>(GetChild(index_collection, index_offset));
    }
    return index;
  }

  oid_t GetIndexCount() {
    oid_t index_count = INVALID_OID;
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      index_count = GetChildrenCount(index_collection);
    }
    return index_count;
  }

  void DropIndex(const oid_t index_id) {
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      DropChildWithID(index_collection, index_id);
    }
  }

  //===--------------------------------------------------------------------===//
  // FOREIGN KEYS
  //===--------------------------------------------------------------------===//

  void AddForeignKey(catalog::ForeignKey *key) {
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      AddChild(foreign_key_collection, static_cast<CatalogObject*>(key));
    }
  }

  catalog::ForeignKey *GetForeignKey(const oid_t key_offset) {
    catalog::ForeignKey *key = nullptr;
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      key = static_cast<catalog::ForeignKey *>(GetChild(foreign_key_collection, key_offset));
    }
    return key;
  }

  oid_t GetForeignKeyCount() {
    oid_t key_count = INVALID_OID;
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      key_count = GetChildrenCount(foreign_key_collection);
    }
    return key_count;
  }

  void DropForeignKey(const oid_t key_offset) {
    {
      std::lock_guard<std::mutex> lock(table_mutex);
      DropChild(foreign_key_collection, key_offset);
    }
  }

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  bool HasPrimaryKey(){
    return has_primary_key;
  }

  bool HasUniqueConstraints(){
    return (unique_constraint_count > 0);
  }

  bool HasForeignKeys(){
    return (GetForeignKeyCount() > 0);
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

  // foreign key collection
  constexpr static oid_t foreign_key_collection = 2;

  // table mutex
  std::mutex table_mutex;

  // has a primary key ?
  std::atomic<bool> has_primary_key = false;

  // # of unique constraints
  std::atomic<oid_t> unique_constraint_count = 0;

};

} // End catalog namespace
} // End peloton namespace

