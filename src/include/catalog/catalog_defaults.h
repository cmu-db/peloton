//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_defaults.h
//
// Identification: src/include/catalog/catalog_defaults.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "common/internal_types.h"

namespace peloton {
namespace catalog {

// System Catalogs imitating Postgres
// (https://www.postgresql.org/docs/9.6/static/catalogs.html)
// Differences are:
// 1. pg_catalog is a catalog schema in each database in Postgres, while it is a
// seperate catalog database in Peloton
// 2. pg_class contains everything similar to a table in Postgres, while Peloton
// uses pg_table only for the table catalog

// Catalog database
#define CATALOG_DATABASE_NAME "pg_catalog"

// Catalog tables
// 4 basic catalog tables
#define DATABASE_CATALOG_NAME "pg_database"
#define TABLE_CATALOG_NAME "pg_table"
#define INDEX_CATALOG_NAME "pg_index"
#define COLUMN_CATALOG_NAME "pg_attribute"

// Local oids from START_OID = 0 to START_OID + OID_OFFSET are reserved
#define OID_OFFSET 100

// Oid mask for each type
#define DATABASE_OID_MASK (static_cast<oid_t>(catalog::CatalogType::DATABASE))
#define TABLE_OID_MASK (static_cast<oid_t>(catalog::CatalogType::TABLE))
#define INDEX_OID_MASK (static_cast<oid_t>(catalog::CatalogType::INDEX))
#define TRIGGER_OID_MASK (static_cast<oid_t>(catalog::CatalogType::TRIGGER))
#define LANGUAGE_OID_MASK (static_cast<oid_t>(catalog::CatalogType::LANGUAGE))
#define PROC_OID_MASK (static_cast<oid_t>(catalog::CatalogType::PROC))

// Reserved pg_catalog database oid
#define CATALOG_DATABASE_OID (0 | DATABASE_OID_MASK)

// Reserved pg_xxx table oid
#define DATABASE_CATALOG_OID (0 | TABLE_OID_MASK)
#define TABLE_CATALOG_OID (1 | TABLE_OID_MASK)
#define INDEX_CATALOG_OID (2 | TABLE_OID_MASK)
#define COLUMN_CATALOG_OID (3 | TABLE_OID_MASK)

// Reserved pg_column index oid
#define COLUMN_CATALOG_PKEY_OID (0 | INDEX_OID_MASK)
#define COLUMN_CATALOG_SKEY0_OID (1 | INDEX_OID_MASK)
#define COLUMN_CATALOG_SKEY1_OID (2 | INDEX_OID_MASK)

// Reserved pg_index index oid
#define INDEX_CATALOG_PKEY_OID (3 | INDEX_OID_MASK)
#define INDEX_CATALOG_SKEY0_OID (4 | INDEX_OID_MASK)
#define INDEX_CATALOG_SKEY1_OID (5 | INDEX_OID_MASK)

// Use upper 8 bits indicating catalog type
#define CATALOG_TYPE_OFFSET 24

enum class CatalogType : uint32_t {
  INVALID = INVALID_TYPE_ID,
  DATABASE = 1 << CATALOG_TYPE_OFFSET,
  TABLE = 2 << CATALOG_TYPE_OFFSET,
  INDEX = 3 << CATALOG_TYPE_OFFSET,
  COLUMN = 4 << CATALOG_TYPE_OFFSET,
  TRIGGER = 5 << CATALOG_TYPE_OFFSET,
  LANGUAGE = 6 << CATALOG_TYPE_OFFSET,
  PROC = 7 << CATALOG_TYPE_OFFSET,
  // To be added
};

}  // namespace catalog
}  // namespace peloton
