//= == ---------------------------------------------------------------------- ==
//
//
//                         Peloton
//
// table_catalog_object.cpp
//
// Identification: src/catalog/table_catalog_object.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/table_catalog_object.h"
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

TableCatalogObject::TableCatalogObject(std::string table_name, oid_t table_oid, oid_t database_oid,
                   Schema *tuple_schema, bool own_schema)
    : AbstractCatalogObject(table_name, table_oid),
      database_oid(database_oid),
      schema_(tuple_schema),
      own_schema_(own_schema) {}

TableCatalogObject::~TableCatalogObject() {
  // clean up tuple schema
  delete schema_;
  return;
}

const std::string TableCatalogObject::GetInfo() const {
  std::stringstream os;

  os << "TableCatalogObject["
     << "Oid=" << oid_ << ", "
     << "Name=" << name_ << ", ";

  os << " -> " << schema_->GetInfo();

  return os.str();
}
}  // end of namespace catalog
}  // end of namespace Peloton