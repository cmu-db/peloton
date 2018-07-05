//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// schema_catalog.cpp
//
// Identification: src/catalog/schema_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Index Group
//
//===----------------------------------------------------------------------===//

#include "catalog/schema_catalog.h"

#include "catalog/catalog.h"
#include "catalog/system_catalogs.h"
#include "concurrency/transaction_context.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/tuple.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

SchemaCatalogEntry::SchemaCatalogEntry(concurrency::TransactionContext *txn,
                                       executor::LogicalTile *tile)
    : schema_oid_(tile->GetValue(0, SchemaCatalog::ColumnId::SCHEMA_OID)
                     .GetAs<oid_t>()),
      schema_name_(
          tile->GetValue(0, SchemaCatalog::ColumnId::SCHEMA_NAME).ToString()),
      txn_(txn) {}

SchemaCatalog::SchemaCatalog(concurrency::TransactionContext *,
                             storage::Database *database,
                             type::AbstractPool *)
    : AbstractCatalog(database,
                      InitializeSchema().release(),
                      SCHEMA_CATALOG_OID,
                      SCHEMA_CATALOG_NAME) {
  // Add indexes for pg_namespace
  AddIndex(SCHEMA_CATALOG_NAME "_pkey",
           SCHEMA_CATALOG_PKEY_OID,
           {ColumnId::SCHEMA_OID},
           IndexConstraintType::PRIMARY_KEY);
  AddIndex(SCHEMA_CATALOG_NAME "_skey0",
           SCHEMA_CATALOG_SKEY0_OID,
           {ColumnId::SCHEMA_NAME},
           IndexConstraintType::UNIQUE);
}

SchemaCatalog::~SchemaCatalog() {}

/*@brief   private function for initialize schema of pg_namespace
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> SchemaCatalog::InitializeSchema() {
  auto schema_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "schema_oid", true);
  schema_id_column.SetNotNull();

  auto schema_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size_, "schema_name", false);
  schema_name_column.SetNotNull();

  std::unique_ptr<catalog::Schema> schema(
      new catalog::Schema({schema_id_column, schema_name_column}));

  schema->AddConstraint(std::make_shared<Constraint>(
      SCHEMA_CATALOG_CON_PKEY_OID, ConstraintType::PRIMARY, "con_primary",
      SCHEMA_CATALOG_OID, std::vector<oid_t>{ColumnId::SCHEMA_OID},
      SCHEMA_CATALOG_PKEY_OID));

  schema->AddConstraint(std::make_shared<catalog::Constraint>(
      SCHEMA_CATALOG_CON_UNI0_OID, ConstraintType::UNIQUE, "con_unique",
      SCHEMA_CATALOG_OID, std::vector<oid_t>{ColumnId::SCHEMA_NAME},
      SCHEMA_CATALOG_SKEY0_OID));

  return schema;
}

bool SchemaCatalog::InsertSchema(concurrency::TransactionContext *txn,
                                 oid_t schema_oid,
                                 const std::string &schema_name,
                                 type::AbstractPool *pool) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(schema_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(schema_name, nullptr);

  tuple->SetValue(SchemaCatalog::ColumnId::SCHEMA_OID, val0, pool);
  tuple->SetValue(SchemaCatalog::ColumnId::SCHEMA_NAME, val1, pool);

  // Insert the tuple
  return InsertTuple(txn, std::move(tuple));
}

bool SchemaCatalog::DeleteSchema(concurrency::TransactionContext *txn,
                                 const std::string &schema_name) {
  oid_t index_offset = IndexId::SKEY_SCHEMA_NAME;  // Index of schema_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(schema_name, nullptr).Copy());

  return DeleteWithIndexScan(txn, index_offset, values);
}

std::shared_ptr<SchemaCatalogEntry> SchemaCatalog::GetSchemaCatalogEntry(
    concurrency::TransactionContext *txn,
    const std::string &schema_name) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // get from pg_namespace, index scan
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::SKEY_SCHEMA_NAME;  // Index of database_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(schema_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto schema_object =
        std::make_shared<SchemaCatalogEntry>(txn, (*result_tiles)[0].get());
    // TODO: we don't have cache for schema object right now
    return schema_object;
  }

  // return empty object if not found
  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
