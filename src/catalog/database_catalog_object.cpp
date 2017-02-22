//= == ---------------------------------------------------------------------- ==
//
//
//                         Peloton
//
// database_catalog_object.cpp
//
// Identification: src/catalog/database_catalog_object.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/database_catalog_object.h"
#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/tuple.h"
#include "type/ephemeral_pool.h"

#include <algorithm>
#include <iostream>

namespace peloton {
namespace catalog {

DatabaseCatalogObject::DatabaseCatalogObject(std::string database_name, oid_t database_oid)
: AbstractCatalogObject(database_name, database_oid) {}

DatabaseCatalogObject::~DatabaseCatalogObject() {}

const std::string DatabaseCatalogObject::GetInfo() const {
  std::stringstream os;

  os << "DatabaseCatalogObject["
     << "Oid=" << oid_ << ", "
     << "Name=" << name_ << ", ";

  return os.str();
}
}  // end of namespace catalog
}  // end of namespace Peloton