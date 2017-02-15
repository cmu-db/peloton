//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog_object.cpp
//
// Identification: src/catalog/index_catalog_object.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/index_catalog_object.h"

#include "index/index.h"
#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/tuple.h"
#include "type/ephemeral_pool.h"

#include "index/scan_optimizer.h"

#include <algorithm>
#include <iostream>

namespace peloton {
namespace catalog {

bool IndexCatalogObject::index_default_visibility = true;

/*
 * GetColumnCount() - Returns the number of indexed columns
 *
 * Please note that this returns the column count of columns in the base
 * table that are indexed, i.e. not the column count of the base table
 */
oid_t IndexCatalogObject::GetColumnCount() const {
  return GetKeySchema()->GetColumnCount();
}

/*
 * Constructor - Initializes tuple key to index mapping
 *
 * NOTE: This metadata object owns key_schema since it is specially
 * constructed for the index
 *
 * However, tuple schema belongs to the table, such that this metadata should
 * not destroy the tuple schema object on destruction
 */
IndexCatalogObject::IndexCatalogObject(
    std::string index_name, oid_t index_oid, oid_t table_oid,
    oid_t database_oid, IndexType index_type,
    IndexConstraintType index_constraint_type,
    const Schema *tuple_schema,
    const Schema *key_schema,
    const std::vector<oid_t> &key_attrs, bool unique_keys)
    : AbstractCatalogObject(index_name, index_oid),
      table_oid(table_oid),
      database_oid(database_oid),
      index_type_(index_type),
      index_constraint_type_(index_constraint_type),
      tuple_schema(tuple_schema),
      key_schema(key_schema),
      key_attrs(key_attrs),
      tuple_attrs(),
      unique_keys(unique_keys),
      visible_(IndexCatalogObject::index_default_visibility) {
  // Push the reverse mapping relation into tuple_attrs which maps
  // tuple key's column into index key's column
  // resize() automatially does allocation, extending and insertion
  tuple_attrs.resize(tuple_schema->GetColumnCount(), INVALID_OID);

  // For those column IDs not mapped, they are set to INVALID_OID
  for (oid_t i = 0; i < key_attrs.size(); i++) {
    // That is the tuple column ID that index key column i is mapped to
    oid_t tuple_column_id = key_attrs[i];

    // The tuple column must be included into key_attrs, otherwise
    // the construction argument is malformed
    PL_ASSERT(tuple_column_id < tuple_attrs.size());

    tuple_attrs[tuple_column_id] = i;
  }

  // Just in case somebody forgets they set our flag to default and
  // was wondering why there indexes weren't working...
  if (visible_ == false) {
    LOG_WARN(
        "Creating IndexCatalogObject for '%s' (%s) but visible flag is set to "
        "false.",
        name_.c_str(), GetInfo().c_str());
  }

  return;
}

IndexCatalogObject::~IndexCatalogObject() {
  // clean up key schema
  delete key_schema;

  // no need to clean the tuple schema
  return;
}

const std::string IndexCatalogObject::GetInfo() const {
  std::stringstream os;

  os << "IndexCatalogObject["
     << "Oid=" << index_oid << ", "
     << "Name=" << name_ << ", "
     << "Type=" << IndexTypeToString(index_type_) << ", "
     << "ConstraintType=" << IndexConstraintTypeToString(index_constraint_type_)
     << ", "
     << "UtilityRatio=" << utility_ratio << ", "
     << "Visible=" << visible_ << "]";

  os << " -> " << key_schema->GetInfo();

  return os.str();
}
}
}
