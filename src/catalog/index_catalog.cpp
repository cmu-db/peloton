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

#include "concurrency/transaction_context.h"
#include "catalog/table_catalog.h"
#include "catalog/column_catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

IndexCatalogObject::IndexCatalogObject(executor::LogicalTile *tile, int tupleId)
    : index_oid(tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEX_OID)
                    .GetAs<oid_t>()),
      index_name(tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEX_NAME)
                     .ToString()),
      table_oid(tile->GetValue(tupleId, IndexCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
      index_type(tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEX_TYPE)
                     .GetAs<IndexType>()),
      index_constraint(
          tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEX_CONSTRAINT)
              .GetAs<IndexConstraintType>()),
      unique_keys(tile->GetValue(tupleId, IndexCatalog::ColumnId::UNIQUE_KEYS)
                      .GetAs<bool>()) {
  std::string attr_str =
      tile->GetValue(tupleId, IndexCatalog::ColumnId::INDEXED_ATTRIBUTES)
          .ToString();
  std::stringstream ss(attr_str.c_str());  // Turn the string into a stream.
  std::string tok;

  while (std::getline(ss, tok, ' ')) {
    key_attrs.push_back(std::stoi(tok));
  }
  LOG_TRACE("the size for indexed key is %lu", key_attrs.size());
}

IndexCatalog *IndexCatalog::GetInstance(storage::Database *pg_catalog,
                                        type::AbstractPool *pool,
                                        concurrency::TransactionContext *txn) {
  static IndexCatalog index_catalog{pg_catalog, pool, txn};
  return &index_catalog;
}

IndexCatalog::IndexCatalog(storage::Database *pg_catalog,
                           type::AbstractPool *pool,
                           concurrency::TransactionContext *txn)
    : AbstractCatalog(INDEX_CATALOG_OID, INDEX_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Add indexes for pg_index
  AddIndex({0}, INDEX_CATALOG_PKEY_OID, INDEX_CATALOG_NAME "_pkey",
           IndexConstraintType::PRIMARY_KEY);
  AddIndex({1}, INDEX_CATALOG_SKEY0_OID, INDEX_CATALOG_NAME "_skey0",
           IndexConstraintType::UNIQUE);
  AddIndex({2}, INDEX_CATALOG_SKEY1_OID, INDEX_CATALOG_NAME "_skey1",
           IndexConstraintType::DEFAULT);

  // Insert columns into pg_attribute
  ColumnCatalog *pg_attribute =
      ColumnCatalog::GetInstance(pg_catalog, pool, txn);

  oid_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    pg_attribute->InsertColumn(INDEX_CATALOG_OID, column.GetName(), column_id,
                               column.GetOffset(), column.GetType(),
                               column.IsInlined(), column.GetConstraints(),
                               pool, txn);
    column_id++;
  }
}

IndexCatalog::~IndexCatalog() {}

/*@brief   private function for initialize schema of pg_index
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> IndexCatalog::InitializeSchema() {
  const std::string not_null_constraint_name = "not_null";
  const std::string primary_key_constraint_name = "primary_key";

  auto index_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "index_oid", true);
  index_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  index_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_name_column = catalog::Column(type::TypeId::VARCHAR, max_name_size,
                                           "index_name", false);
  index_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_type_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "index_type", true);
  index_type_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_constraint_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "index_constraint", true);
  index_constraint_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto unique_keys = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "unique_keys", true);
  unique_keys.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto indexed_attributes_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "indexed_attributes", false);
  indexed_attributes_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));
  std::unique_ptr<catalog::Schema> index_schema(new catalog::Schema(
      {index_id_column, index_name_column, table_id_column, index_type_column,
       index_constraint_column, unique_keys, indexed_attributes_column}));
  return index_schema;
}

bool IndexCatalog::InsertIndex(oid_t index_oid, const std::string &index_name,
                               oid_t table_oid, IndexType index_type,
                               IndexConstraintType index_constraint,
                               bool unique_keys, std::vector<oid_t> indekeys,
                               type::AbstractPool *pool,
                               concurrency::TransactionContext *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(index_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(index_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val3 = type::ValueFactory::GetIntegerValue(static_cast<int>(index_type));
  auto val4 =
      type::ValueFactory::GetIntegerValue(static_cast<int>(index_constraint));
  auto val5 = type::ValueFactory::GetBooleanValue(unique_keys);

  std::stringstream os;
  for (oid_t indkey : indekeys) os << std::to_string(indkey) << " ";
  auto val6 = type::ValueFactory::GetVarcharValue(os.str(), nullptr);

  tuple->SetValue(IndexCatalog::ColumnId::INDEX_OID, val0, pool);
  tuple->SetValue(IndexCatalog::ColumnId::INDEX_NAME, val1, pool);
  tuple->SetValue(IndexCatalog::ColumnId::TABLE_OID, val2, pool);
  tuple->SetValue(IndexCatalog::ColumnId::INDEX_TYPE, val3, pool);
  tuple->SetValue(IndexCatalog::ColumnId::INDEX_CONSTRAINT, val4, pool);
  tuple->SetValue(IndexCatalog::ColumnId::UNIQUE_KEYS, val5, pool);
  tuple->SetValue(IndexCatalog::ColumnId::INDEXED_ATTRIBUTES, val6, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool IndexCatalog::DeleteIndex(oid_t index_oid, concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto index_object = txn->catalog_cache.GetCachedIndexObject(index_oid);
  if (index_object) {
    auto table_object =
        txn->catalog_cache.GetCachedTableObject(index_object->GetTableOid());
    table_object->EvictAllIndexObjects();
  }

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::shared_ptr<IndexCatalogObject> IndexCatalog::GetIndexObject(
    oid_t index_oid, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto index_object = txn->catalog_cache.GetCachedIndexObject(index_oid);
  if (index_object) {
    return index_object;
  }

  // cache miss, get from pg_index
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto index_object =
        std::make_shared<IndexCatalogObject>((*result_tiles)[0].get());
    // fetch all indexes into table object (cannot use the above index object)
    auto table_object = TableCatalog::GetInstance()->GetTableObject(
        index_object->GetTableOid(), txn);
    PL_ASSERT(table_object &&
              table_object->GetTableOid() == index_object->GetTableOid());
    return table_object->GetIndexObject(index_oid);
  } else {
    LOG_DEBUG("Found %lu index with oid %u", result_tiles->size(), index_oid);
  }

  // return empty object if not found
  return nullptr;
}

std::shared_ptr<IndexCatalogObject> IndexCatalog::GetIndexObject(
    const std::string &index_name, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto index_object = txn->catalog_cache.GetCachedIndexObject(index_name);
  if (index_object) {
    return index_object;
  }

  // cache miss, get from pg_index
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SKEY_INDEX_NAME;  // Index of index_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(index_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto index_object =
        std::make_shared<IndexCatalogObject>((*result_tiles)[0].get());
    // fetch all indexes into table object (cannot use the above index object)
    auto table_object = TableCatalog::GetInstance()->GetTableObject(
        index_object->GetTableOid(), txn);
    PL_ASSERT(table_object &&
              table_object->GetTableOid() == index_object->GetTableOid());
    return table_object->GetIndexObject(index_name);
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
const std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>>
IndexCatalog::GetIndexObjects(oid_t table_oid, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto table_object =
      TableCatalog::GetInstance()->GetTableObject(table_oid, txn);
  PL_ASSERT(table_object && table_object->GetTableOid() == table_oid);
  auto index_objects = table_object->GetIndexObjects(true);
  if (index_objects.empty() == false) return index_objects;

  // cache miss, get from pg_index
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      auto index_object =
          std::make_shared<IndexCatalogObject>(tile.get(), tuple_id);
      table_object->InsertIndexObject(index_object);
    }
  }

  return table_object->GetIndexObjects();
}

}  // namespace catalog
}  // namespace peloton
