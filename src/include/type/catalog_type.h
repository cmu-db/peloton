//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_type.h
//
// Identification: src/include/type/catalog_type.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/types.h"

namespace peloton {
namespace type {

// System Catalogs imitating Postgres
// (https://www.postgresql.org/docs/9.6/static/catalogs.html)
// Differences are:
// 1. pg_catalog is a catalog schema in each database in Postgres, while it is a
// seperate catalog database in Peloton
// 2. pg_class contains everything similar to a table in Postgres, while Peloton
// uses pg_table only for the table catalog

#define CATALOG_DATABASE_NAME "pg_catalog"
#define DATABASE_CATALOG_NAME "pg_database"
#define TABLE_CATALOG_NAME "pg_table"
#define INDEX_CATALOG_NAME "pg_index"
#define COLUMN_CATALOG_NAME "pg_attribute"

// Oid from START_OID = 0 to START_OID + OID_OFFSET are reserved
#define OID_OFFSET 1000

// pg_xxx table oid
#define DATABASE_CATALOG_OID (0 | static_cast<oid_t>(type::CatalogType::TABLE))
#define TABLE_CATALOG_OID (1 | static_cast<oid_t>(type::CatalogType::TABLE))
#define INDEX_CATALOG_OID (2 | static_cast<oid_t>(type::CatalogType::TABLE))
#define COLUMN_CATALOG_OID (3 | static_cast<oid_t>(type::CatalogType::TABLE))

#define CATALOG_TYPE_OFFSET 24

enum class CatalogType : uint32_t {
  INVALID = INVALID_TYPE_ID,
  DATABASE = 0,
  TABLE = 1 << CATALOG_TYPE_OFFSET,
  INDEX = 2 << CATALOG_TYPE_OFFSET,
  COLUMN = 3 << CATALOG_TYPE_OFFSET,
  // To be added
};
}
}
