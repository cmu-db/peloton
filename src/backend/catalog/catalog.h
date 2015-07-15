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

  // Get a string representation of this catalog
  friend std::ostream& operator<<(std::ostream& os, const Catalog& catalog);

};

} // End catalog namespace
} // End peloton namespace
