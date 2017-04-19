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
#include "catalog/column_catalog.h"

namespace peloton {
namespace catalog {

IndexCatalog *IndexCatalog::GetInstance(storage::Database *pg_catalog,
                                        type::AbstractPool *pool,
                                        concurrency::Transaction *txn) {
  static std::unique_ptr<IndexCatalog> index_catalog(
      new IndexCatalog(pg_catalog, pool, txn));

  return index_catalog.get();
}

IndexCatalog::IndexCatalog(storage::Database *pg_catalog,
                           type::AbstractPool *pool,
                           concurrency::Transaction *txn)
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
  ColumnCatalog *pg_attribute = ColumnCatalog::GetInstance(pg_catalog, pool, txn);

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
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "index_oid", true);
  index_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  index_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_name_column =
      catalog::Column(type::Type::VARCHAR, max_name_size, "index_name", false);
  index_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "table_oid", true);
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_type_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "index_type", true);
  index_type_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_constraint_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "index_constraint", true);
  index_constraint_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto unique_keys = catalog::Column(
      type::Type::BOOLEAN, type::Type::GetTypeSize(type::Type::BOOLEAN),
      "unique_keys", true);
  unique_keys.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto indexed_attributes_column = catalog::Column(
      type::Type::VARCHAR, max_name_size, "indexed_attributes", false);
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
                               concurrency::Transaction *txn) {
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

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);
  tuple->SetValue(2, val2, pool);
  tuple->SetValue(3, val3, pool);
  tuple->SetValue(4, val4, pool);
  tuple->SetValue(5, val5, pool);
  tuple->SetValue(6, val6, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool IndexCatalog::DeleteIndex(oid_t index_oid, concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::string IndexCatalog::GetIndexName(oid_t index_oid,
                                       concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // index_name
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string index_name;
  PL_ASSERT(result_tiles->size() <= 1);  // index_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      index_name = (*result_tiles)[0]
                       ->GetValue(0, 0)
                       .ToString();  // After projection left 1 column
    }
  }

  return index_name;
}

oid_t IndexCatalog::GetTableOid(oid_t index_oid,
                                concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({2});  // table_oid
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t table_oid = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      table_oid = (*result_tiles)[0]
                      ->GetValue(0, 0)
                      .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return table_oid;
}

IndexType IndexCatalog::GetIndexType(oid_t index_oid,
                                     concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({3});  // index_type
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  IndexType index_type = IndexType::INVALID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      index_type = static_cast<IndexType>(
          (*result_tiles)[0]
              ->GetValue(0, 0)
              .GetAs<int>());  // After projection left 1 column
    }
  }

  return index_type;
}

IndexConstraintType IndexCatalog::GetIndexConstraint(
    oid_t index_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({4});  // index_constraint
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  IndexConstraintType index_constraint = IndexConstraintType::INVALID;
  PL_ASSERT(result_tiles->size() <= 1);  // index_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      index_constraint = static_cast<IndexConstraintType>(
          (*result_tiles)[0]
              ->GetValue(0, 0)
              .GetAs<int>());  // After projection left 1 column
    }
  }

  return index_constraint;
}

bool IndexCatalog::IsUniqueKeys(oid_t index_oid,
                                concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({5});  // unique_keys
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  bool unique_keys = false;
  PL_ASSERT(result_tiles->size() <= 1);  // index_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      unique_keys = (*result_tiles)[0]
                        ->GetValue(0, 0)
                        .GetAs<bool>();  // After projection left 1 column
    }
  }

  return unique_keys;
}

/*@brief   get all index records from the same table
* this function may be useful when calling DropTable
* @param   table_oid
* @param   txn  Transaction
* @return  a vector of index oid
*/
std::vector<oid_t> IndexCatalog::GetIndexOids(oid_t table_oid,
                                              concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // index_oid
  oid_t index_offset = 2;              // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::vector<oid_t> index_oids;
  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      index_oids.emplace_back(
          tile->GetValue(tuple_id, 0)
              .GetAs<oid_t>());  // After projection left 1 column
    }
  }

  return index_oids;
}

oid_t IndexCatalog::GetIndexOid(const std::string &index_name,
                                concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // index_oid
  oid_t index_offset = 1;              // Index of index_name & table_oid
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(index_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t index_oid = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // index_name & table_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      index_oid = (*result_tiles)[0]
                      ->GetValue(0, 0)
                      .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return index_oid;
}

/*@brief   return all the columns this index indexed
* @param   index_oid
* @param   txn  Transaction
* @return  a vector of column oid(logical position)
*/
std::vector<oid_t> IndexCatalog::GetIndexedAttributes(
    oid_t index_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({6});  // Indexed attributes
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  std::vector<oid_t> key_attrs;
  std::string temp;
  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  PL_ASSERT(result_tiles->size() <= 1);  // index_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      temp = (*result_tiles)[0]->GetValue(0, 0).ToString();
    }
  }
  LOG_TRACE("the string value for index keys is %s", temp.c_str());
  // using " " as delimiter to split up string and turn into vector of oid_t
  std::stringstream os(temp.c_str());  // Turn the string into a stream.
  std::string tok;

  while (std::getline(os, tok, ' ')) {
    key_attrs.push_back(std::stoi(tok));
  }
  LOG_TRACE("the size for indexed key is %lu", key_attrs.size());
  return key_attrs;
}

}  // End catalog namespace
}  // End peloton namespace
