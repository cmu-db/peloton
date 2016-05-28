//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_table.cpp
//
// Identification: src/backend/storage/abstract_table.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/abstract_table.h"

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"

#include <mutex>
#include "backend/gc/gc_manager_factory.h"

namespace peloton {
namespace storage {

AbstractTable::AbstractTable(oid_t database_oid, oid_t table_oid,
                             std::string table_name, catalog::Schema *schema,
                             bool own_schema)
    : database_oid(database_oid),
      table_oid(table_oid),
      table_name(table_name),
      schema(schema),
      own_schema_(own_schema) {
  // Register GC if using cooperative GC or vacuum GC
  if (gc::GCManagerFactory::GetGCType() == GC_TYPE_CO) {
    auto *gc_manager = dynamic_cast<gc::Cooperative_GCManager*>(&gc::GCManagerFactory::GetInstance());
    assert(gc_manager != nullptr);
    gc_manager->RegisterTable(table_oid);
  } else if (gc::GCManagerFactory::GetGCType() == GC_TYPE_VACUUM) {
    auto *gc_manager = dynamic_cast<gc::Vacuum_GCManager*>(&gc::GCManagerFactory::GetInstance());
    assert(gc_manager != nullptr);
    gc_manager->RegisterTable(table_oid);
  }
}

AbstractTable::~AbstractTable() {
  // clean up schema
  if (own_schema_) delete schema;
}

const std::string AbstractTable::GetInfo() const { return "AbstractTable"; }

}  // End storage namespace
}  // End peloton namespace
