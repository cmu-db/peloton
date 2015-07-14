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

#include "backend/catalog/catalog_object.h"
#include "backend/catalog/table.h"

#include <iostream>
#include <mutex>

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
                 database_name,
                 parent,
                 root) {
  }

  // Get a string representation of this database
  friend std::ostream& operator<<(std::ostream& os, const Database& database);

};

} // End catalog namespace
} // End peloton namespace
