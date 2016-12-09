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

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "gc/gc_manager_factory.h"
#include "index/index.h"

namespace peloton {
namespace storage {

AbstractTable::AbstractTable(id_t table_oid, catalog::Schema *schema,
                             bool own_schema)
    : table_oid(table_oid), schema(schema), own_schema_(own_schema) {
  // Register table to GC manager.
  auto *gc_manager = &gc::GCManagerFactory::GetInstance();
  assert(gc_manager != nullptr);
  gc_manager->RegisterTable(table_oid);
}

AbstractTable::~AbstractTable() {
  // clean up schema
  if (own_schema_) delete schema;
}

}  // End storage namespace
}  // End peloton namespace
