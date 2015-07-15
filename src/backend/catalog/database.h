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

  void Lock() {
    database_mutex.lock();
  }

  void Unlock() {
    database_mutex.unlock();
  }

  // Get a string representation of this database
  friend std::ostream& operator<<(std::ostream& os, const Database& database);

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  std::string database_name;

  std::mutex database_mutex;
};

} // End catalog namespace
} // End peloton namespace
