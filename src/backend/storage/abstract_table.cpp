/*-------------------------------------------------------------------------
 *
 * abstract_table.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/storage/abstract_table.h"

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tile_group_factory.h"

#include <mutex>

namespace peloton {
namespace storage {

AbstractTable::AbstractTable(oid_t table_oid, std::string table_name,
                             catalog::Schema *schema)
    : table_oid(table_oid),
      database_oid(INVALID_OID),
      table_name(table_name),
      schema(schema) {}

AbstractTable::~AbstractTable() {
  // clean up schema
  delete schema;
}

}  // End storage namespace
}  // End peloton namespace
