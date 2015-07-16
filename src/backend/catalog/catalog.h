/*-------------------------------------------------------------------------
 *
 * catalog.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/catalog.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/catalog_object.h"
#include "backend/catalog/database.h"

#include <iostream>
#include <algorithm>
#include <mutex>

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//

/**
 * Access gateway for all catalog objects
 * This keeps track of all the databases in the system.
 */
class Catalog : CatalogObject {

 public:

  // Get the singleton catalog instance
  static Catalog& GetInstance();

  Catalog()
  : CatalogObject(INVALID_OID,
                  nullptr,
                  this) {
  }

  void AddDatabase(catalog::Database *database) {
    {
      std::lock_guard<std::mutex> lock(catalog_mutex);
      AddChild(database_collection, static_cast<CatalogObject*>(database));
    }
  }

  catalog::Database *GetDatabase(const oid_t database_id) {
    catalog::Database *database = nullptr;
    {
      std::lock_guard<std::mutex> lock(catalog_mutex);
      database = static_cast<catalog::Database *>(GetChildWithID(database_collection, database_id));
    }
    return database;
  }

  void DropDatabase(const oid_t database_id) {
    {
      std::lock_guard<std::mutex> lock(catalog_mutex);
      DropChildWithID(database_collection, database_id);
    }
  }

  // Get a string representation of this catalog
  friend std::ostream& operator<<(std::ostream& os, const Catalog& catalog);

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // database collection
  constexpr static oid_t database_collection = 0;

  // catalog mutex
  std::mutex catalog_mutex;

};

} // End catalog namespace
} // End peloton namespace
