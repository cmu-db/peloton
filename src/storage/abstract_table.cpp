//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_table.cpp
//
// Identification: src/storage/abstract_table.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "storage/abstract_table.h"

#include "common/exception.h"
#include "common/logger.h"
#include "index/index.h"
#include "catalog/manager.h"
#include "catalog/schema.h"

#include <mutex>

namespace peloton {
namespace storage {

AbstractTable::AbstractTable(oid_t database_oid, oid_t table_oid,
                             std::string table_name, catalog::Schema *schema,
                             bool own_schema)
    : database_oid(database_oid),
      table_oid(table_oid),
      table_name(table_name),
      schema(schema),
      own_schema_(own_schema) {}

AbstractTable::~AbstractTable() {
  // clean up schema
  if (own_schema_) delete schema;
}

const std::string AbstractTable::GetInfo() const { return "AbstractTable"; }

}  // End storage namespace
}  // End peloton namespace
