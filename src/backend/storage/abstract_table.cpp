//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_table.cpp
//
// Identification: src/backend/storage/abstract_table.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/abstract_table.h"

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tile_group_factory.h"

#include <mutex>

namespace peloton {
namespace storage {

AbstractTable::AbstractTable(oid_t database_oid, oid_t table_oid, std::string table_name,
                             catalog::Schema *schema, bool own_schema)
    : database_oid(database_oid),
      table_oid(table_oid),
      table_name(table_name),
      schema(schema),
      own_schema_(own_schema){}

AbstractTable::~AbstractTable() {
  // clean up schema
  if(own_schema_)
    delete schema;
}

}  // End storage namespace
}  // End peloton namespace
