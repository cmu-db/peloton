/*-------------------------------------------------------------------------
 *
 * database.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <iostream>
#include <mutex>

#include "backend/catalog/catalog_object.h"
#include "backend/catalog/table.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Database
//===--------------------------------------------------------------------===//

class Database : public CatalogObject {

 public:

  Database(oid_t database_oid,
           std::string database_name,
           CatalogObject *parent,
           CatalogObject *root)
 : CatalogObject(database_oid,
                 parent,
                 root),
                 database_name(database_name){
  }

  std::string GetName() const {
    return database_name;
  }

  void AddTable(catalog::Table *table) {
    {
      std::lock_guard<std::mutex> lock(database_mutex);
      AddChild(table_collection, static_cast<CatalogObject*>(table));
    }
  }

  catalog::Table *GetTable(const oid_t table_id) {
    catalog::Table *table = nullptr;
    {
      std::lock_guard<std::mutex> lock(database_mutex);
      table = static_cast<catalog::Table *>(GetChildWithID(table_collection, table_id));
    }
    return table;
  }

  void DropTable(const oid_t table_id) {
    {
      std::lock_guard<std::mutex> lock(database_mutex);
      DropChildWithID(table_collection, table_id);
    }
  }

  // Get a string representation of this database
  friend std::ostream& operator<<(std::ostream& os, const Database& database);

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // database name
  std::string database_name;

  // table collection
  constexpr static oid_t table_collection = 0;

  // database mutex
  std::mutex database_mutex;

};

} // End catalog namespace
} // End peloton namespace
