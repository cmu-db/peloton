//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog.cpp
//
// Identification: src/catalog/index_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Index Group
//
//===----------------------------------------------------------------------===//

#include "catalog/index_catalog.h"

#include <sstream>

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

IndexCatalogEntry::IndexCatalogEntry(executor::LogicalTile *tile, int tupleId)
    : index_oid_(tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEX_OID)
                    .GetAs<oid_t>()),
      index_name_(tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEX_NAME)
                     .ToString()),
      table_oid_(tile->GetValue(tupleId, IndexCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
      schema_name_(tile->GetValue(tupleId, IndexCatalog::ColumnId::SCHEMA_NAME)
                      .ToString()),
      index_type_(tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEX_TYPE)
                     .GetAs<IndexType>()),
      index_constraint_(
          tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEX_CONSTRAINT)
              .GetAs<IndexConstraintType>()),
      unique_keys_(tile->GetValue(tupleId, IndexCatalog::ColumnId::UNIQUE_KEYS)
                      .GetAs<bool>()) {
  std::string attr_str =
      tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEXED_ATTRIBUTES)
          .ToString();
  std::stringstream ss(attr_str.c_str());  // Turn the string into a stream.
  std::string tok;

  while (std::getline(ss, tok, ' ')) {
    key_attrs_.push_back(std::stoi(tok));
  }
  LOG_TRACE("the size for indexed key is %lu", key_attrs_.size());
}

IndexCatalog::IndexCatalog(concurrency::TransactionContext *,
                           storage::Database *pg_catalog,
                           type::AbstractPool *)
    : AbstractCatalog(pg_catalog,
                      InitializeSchema().release(),
                      INDEX_CATALOG_OID,
                      INDEX_CATALOG_NAME) {
  // Add indexes for pg_index
  AddIndex(INDEX_CATALOG_NAME "_pkey",
           INDEX_CATALOG_PKEY_OID,
           {ColumnId::INDEX_OID},
           IndexConstraintType::PRIMARY_KEY);
  AddIndex(INDEX_CATALOG_NAME "_skey0",
           INDEX_CATALOG_SKEY0_OID,
           {ColumnId::INDEX_NAME, ColumnId::SCHEMA_NAME},
           IndexConstraintType::UNIQUE);
  AddIndex(INDEX_CATALOG_NAME "_skey1",
           INDEX_CATALOG_SKEY1_OID,
           {ColumnId::TABLE_OID},
           IndexConstraintType::DEFAULT);
}

IndexCatalog::~IndexCatalog() {}

/*@brief   private function for initialize schema of pg_index
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> IndexCatalog::InitializeSchema() {
  auto index_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "index_oid", true);
  index_id_column.SetNotNull();

  auto index_name_column = catalog::Column(type::TypeId::VARCHAR, max_name_size_,
                                           "index_name", false);
  index_name_column.SetNotNull();

  auto table_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_id_column.SetNotNull();

  auto schema_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size_, "schema_name", false);
  schema_name_column.SetNotNull();


  auto index_type_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "index_type", true);
  index_type_column.SetNotNull();

  auto index_constraint_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "index_constraint", true);
  index_constraint_column.SetNotNull();

  auto unique_keys = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "unique_keys", true);
  unique_keys.SetNotNull();

  auto indexed_attributes_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size_, "indexed_attributes", false);
  indexed_attributes_column.SetNotNull();


  std::unique_ptr<catalog::Schema> index_schema(new catalog::Schema(
      {index_id_column, index_name_column, table_id_column, schema_name_column,
       index_type_column, index_constraint_column, unique_keys,
       indexed_attributes_column}));

  index_schema->AddConstraint(std::make_shared<catalog::Constraint>(
      INDEX_CATALOG_CON_PKEY_OID, ConstraintType::PRIMARY, "con_primary",
      INDEX_CATALOG_OID, std::vector<oid_t>{ColumnId::INDEX_OID},
      INDEX_CATALOG_PKEY_OID));

  index_schema->AddConstraint(std::make_shared<catalog::Constraint>(
      INDEX_CATALOG_CON_UNI0_OID, ConstraintType::UNIQUE, "con_unique",
      INDEX_CATALOG_OID, std::vector<oid_t>{ColumnId::INDEX_NAME, ColumnId::SCHEMA_NAME},
      INDEX_CATALOG_SKEY0_OID));

  return index_schema;
}

bool IndexCatalog::InsertIndex(concurrency::TransactionContext *txn,
                               const std::string &schema_name,
                               oid_t table_oid,
                               oid_t index_oid,
                               const std::string &index_name,
                               IndexType index_type,
                               IndexConstraintType index_constraint,
                               bool unique_keys,
                               std::vector<oid_t> index_keys,
                               type::AbstractPool *pool) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(index_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(index_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val3 = type::ValueFactory::GetVarcharValue(schema_name, nullptr);
  auto val4 = type::ValueFactory::GetIntegerValue(static_cast<int>(index_type));
  auto val5 =
      type::ValueFactory::GetIntegerValue(static_cast<int>(index_constraint));
  auto val6 = type::ValueFactory::GetBooleanValue(unique_keys);

  std::stringstream os;
  for (oid_t indkey : index_keys) os << std::to_string(indkey) << " ";
  auto val7 = type::ValueFactory::GetVarcharValue(os.str(), nullptr);

  tuple->SetValue(IndexCatalog::ColumnId::INDEX_OID, val0, pool);
  tuple->SetValue(IndexCatalog::ColumnId::INDEX_NAME, val1, pool);
  tuple->SetValue(IndexCatalog::ColumnId::TABLE_OID, val2, pool);
  tuple->SetValue(IndexCatalog::ColumnId::SCHEMA_NAME, val3, pool);
  tuple->SetValue(IndexCatalog::ColumnId::INDEX_TYPE, val4, pool);
  tuple->SetValue(IndexCatalog::ColumnId::INDEX_CONSTRAINT, val5, pool);
  tuple->SetValue(IndexCatalog::ColumnId::UNIQUE_KEYS, val6, pool);
  tuple->SetValue(IndexCatalog::ColumnId::INDEXED_ATTRIBUTES, val7, pool);

  // Insert the tuple
  return InsertTuple(txn, std::move(tuple));
}

bool IndexCatalog::DeleteIndex(concurrency::TransactionContext *txn,
                               oid_t database_oid,
                               oid_t index_oid) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto index_object = txn->catalog_cache.GetCachedIndexObject(database_oid,
                                                              index_oid);
  if (index_object) {
    auto table_object =
        txn->catalog_cache.GetCachedTableObject(database_oid,
                                                index_object->GetTableOid());
    table_object->EvictAllIndexCatalogEntries();
  }

  return DeleteWithIndexScan(txn, index_offset, values);
}

std::shared_ptr<IndexCatalogEntry> IndexCatalog::GetIndexCatalogEntry(
    concurrency::TransactionContext *txn,
    oid_t database_oid,
    oid_t index_oid) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto index_object = txn->catalog_cache.GetCachedIndexObject(database_oid,
                                                              index_oid);
  if (index_object) {
    return index_object;
  }

  // cache miss, get from pg_index
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto index_object =
        std::make_shared<IndexCatalogEntry>((*result_tiles)[0].get());
    // fetch all indexes into table object (cannot use the above index object)
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid)
                        ->GetTableCatalog();
    auto table_object =
        pg_table->GetTableCatalogEntry(txn, index_object->GetTableOid());
    PELOTON_ASSERT(table_object &&
              table_object->GetTableOid() == index_object->GetTableOid());
    return table_object->GetIndexCatalogEntries(index_oid);
  } else {
    LOG_DEBUG("Found %lu index with oid %u", result_tiles->size(), index_oid);
  }

  // return empty object if not found
  return nullptr;
}

std::shared_ptr<IndexCatalogEntry> IndexCatalog::GetIndexCatalogEntry(
    concurrency::TransactionContext *txn,
    const std::string &database_name,
    const std::string &schema_name,
    const std::string &index_name) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto index_object =
      txn->catalog_cache.GetCachedIndexObject(database_name,
                                              schema_name,
                                              index_name);
  if (index_object) {
    return index_object;
  }

  // cache miss, get from pg_index
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset =
      IndexId::SKEY_INDEX_NAME;  // Index of index_name & schema_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(index_name, nullptr).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(schema_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto index_object =
        std::make_shared<IndexCatalogEntry>((*result_tiles)[0].get());
    // fetch all indexes into table object (cannot use the above index object)
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    auto table_object =
        pg_table->GetTableCatalogEntry(txn, index_object->GetTableOid());
    PELOTON_ASSERT(table_object &&
              table_object->GetTableOid() == index_object->GetTableOid());
    return table_object->GetIndexCatalogEntry(index_name);
  } else {
    LOG_DEBUG("Found %lu index with name %s", result_tiles->size(),
              index_name.c_str());
  }

  // return empty object if not found
  return nullptr;
}

/*@brief   get all index records from the same table
 * this function may be useful when calling DropTable
 * @param   table_oid
 * @param   txn  TransactionContext
 * @return  a vector of index catalog objects
 */
const std::unordered_map<oid_t,
                         std::shared_ptr<IndexCatalogEntry>>
IndexCatalog::GetIndexCatalogEntries(
    concurrency::TransactionContext *txn,
    oid_t table_oid) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);
  auto index_objects = table_object->GetIndexCatalogEntries(true);
  if (index_objects.empty() == false) return index_objects;

  // cache miss, get from pg_index
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      auto index_object =
          std::make_shared<IndexCatalogEntry>(tile.get(), tuple_id);
      table_object->InsertIndexCatalogEntry(index_object);
    }
  }

  return table_object->GetIndexCatalogEntries();
}

}  // namespace catalog
}  // namespace peloton
