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
// 1. pg_class contains everything similar to a table in Postgres, while Peloton
// uses pg_table only for the table catalog

// Catalog database
#define CATALOG_DATABASE_NAME "peloton"

// Catalog tables
// 5 basic catalog tables
#define DATABASE_CATALOG_NAME "pg_database"
#define SCHEMA_CATALOG_NAME "pg_namespace"
#define TABLE_CATALOG_NAME "pg_table"
#define INDEX_CATALOG_NAME "pg_index"
#define COLUMN_CATALOG_NAME "pg_attribute"

// Local oids from START_OID = 0 to START_OID + OID_OFFSET are reserved
#define OID_OFFSET 100
#define CATALOG_TABLES_COUNT 8

// Oid mask for each type
#define DATABASE_OID_MASK (static_cast<oid_t>(catalog::CatalogType::DATABASE))
#define SCHEMA_OID_MASK (static_cast<oid_t>(catalog::CatalogType::SCHEMA))
#define TABLE_OID_MASK (static_cast<oid_t>(catalog::CatalogType::TABLE))
#define INDEX_OID_MASK (static_cast<oid_t>(catalog::CatalogType::INDEX))
#define TRIGGER_OID_MASK (static_cast<oid_t>(catalog::CatalogType::TRIGGER))
#define LANGUAGE_OID_MASK (static_cast<oid_t>(catalog::CatalogType::LANGUAGE))
#define PROC_OID_MASK (static_cast<oid_t>(catalog::CatalogType::PROC))

// Reserved peloton database oid
#define CATALOG_DATABASE_OID (0 | DATABASE_OID_MASK)

// Reserved schema oid
// "public" for default schema, and "pg_catalog" schema for catalog tables
#define CATALOG_SCHEMA_OID (0 | SCHEMA_OID_MASK)
#define DEFUALT_SCHEMA_OID (1 | SCHEMA_OID_MASK)
#define CATALOG_SCHEMA_NAME "pg_catalog"
#define DEFUALT_SCHEMA_NAME "public"

// Reserved pg_xxx table oid
#define DATABASE_CATALOG_OID (0 | TABLE_OID_MASK)
#define SCHEMA_CATALOG_OID (1 | TABLE_OID_MASK)
#define TABLE_CATALOG_OID (2 | TABLE_OID_MASK)
#define INDEX_CATALOG_OID (3 | TABLE_OID_MASK)
#define COLUMN_CATALOG_OID (4 | TABLE_OID_MASK)

// Reserved pg_column index oid
#define COLUMN_CATALOG_PKEY_OID (0 | INDEX_OID_MASK)
#define COLUMN_CATALOG_SKEY0_OID (1 | INDEX_OID_MASK)
#define COLUMN_CATALOG_SKEY1_OID (2 | INDEX_OID_MASK)

// Reserved pg_index index oid
#define INDEX_CATALOG_PKEY_OID (3 | INDEX_OID_MASK)
#define INDEX_CATALOG_SKEY0_OID (4 | INDEX_OID_MASK)
#define INDEX_CATALOG_SKEY1_OID (5 | INDEX_OID_MASK)

// Reserved pg_database index oid
#define DATABASE_CATALOG_PKEY_OID (6 | INDEX_OID_MASK)
#define DATABASE_CATALOG_SKEY0_OID (7 | INDEX_OID_MASK)

// Reserved pg_namespace index oid
#define SCHEMA_CATALOG_PKEY_OID (8 | INDEX_OID_MASK)
#define SCHEMA_CATALOG_SKEY0_OID (9 | INDEX_OID_MASK)

// Reserved pg_table index oid
#define TABLE_CATALOG_PKEY_OID (10 | INDEX_OID_MASK)
#define TABLE_CATALOG_SKEY0_OID (11 | INDEX_OID_MASK)
#define TABLE_CATALOG_SKEY1_OID (12 | INDEX_OID_MASK)

// Use upper 8 bits indicating catalog type
#define CATALOG_TYPE_OFFSET 24

enum class CatalogType : uint32_t {
  INVALID = INVALID_TYPE_ID,
  DATABASE = 1 << CATALOG_TYPE_OFFSET,
  SCHEMA = 2 << CATALOG_TYPE_OFFSET,
  TABLE = 3 << CATALOG_TYPE_OFFSET,
  INDEX = 4 << CATALOG_TYPE_OFFSET,
  COLUMN = 5 << CATALOG_TYPE_OFFSET,
  TRIGGER = 6 << CATALOG_TYPE_OFFSET,
  LANGUAGE = 7 << CATALOG_TYPE_OFFSET,
  PROC = 8 << CATALOG_TYPE_OFFSET,
  // To be added
};

}  // namespace catalog
}  // namespace peloton
