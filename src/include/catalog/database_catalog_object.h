//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog.h
//
// Identification: src/include/catalog_objects.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include "catalog/abstract_catalog_object.h"
#include "catalog/schema.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/printable.h"
#include "type/abstract_pool.h"
#include "type/types.h"
#include "type/value.h"

namespace peloton {
namespace catalog {
/*
 * class DatabaseCatalogObject - Holds metadata of an table object
 *
 * The DatabaseCatalog object maintains the database oid, table oid,
 * table name and corresponding tuple schema.
 */

class DatabaseCatalogObject : public AbstractCatalogObject {
  // Don't allow to call default constructor
  DatabaseCatalogObject() = delete;

 public:
  DatabaseCatalogObject(std::string database_name, oid_t database_oid)
      : AbstractCatalogObject(database_name, database_oid) {}

  ~DatabaseCatalogObject();

  /*
   * GetInfo() - Get a string representation for debugging
   */
  const std::string GetInfo() const;
};
}  // end of namespace catalog
}  // end of namespace Peloton