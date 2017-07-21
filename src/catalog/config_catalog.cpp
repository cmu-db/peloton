//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// config_catalog.cpp
//
// Identification: src/catalog/config_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/config_catalog.h"

namespace peloton {
namespace catalog {

ConfigCatalog *ConfigCatalog::GetInstance(concurrency::Transaction *txn) {
  static std::unique_ptr<ConfigCatalog> config_catalog(new ConfigCatalog(txn));
  return config_catalog.get();
}

ConfigCatalog::ConfigCatalog(concurrency::Transaction *txn)
        : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                                  "." CONFIG_CATALOG_NAME
                                  " ("
                                  "name   VARCHAR NOT NULL, "
                                  "value  VARCHAR NOT NULL, "
                                  "value_type   VARCHAR NOT NULL, "
                                  "description  VARCHAR, "
                                  "min_value    VARCHAR, "
                                  "max_value    VARCHAR, "
                                  "default_value    VARCHAR NOT NULL, "
                                  "is_mutable   BOOL NOT NULL, "
                                  "is_persistent  BOOL NOT NULL);",
                          txn) {
  // Add secondary index here if necessary
  Catalog::GetInstance()->CreateIndex(
          CATALOG_DATABASE_NAME, CONFIG_CATALOG_NAME,
          {"name"}, CONFIG_CATALOG_NAME "_skey0",
          false, IndexType::BWTREE, txn);
}

ConfigCatalog::~ConfigCatalog() {}

bool ConfigCatalog::InsertConfig(const std::string &name, const std::string &value,
                                 type::TypeId value_type, const std::string &description,
                                 const std::string &min_value, const std::string &max_value,
                                 const std::string &default_value,
                                 bool is_mutable, bool is_persistent,
                                 type::AbstractPool *pool, concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetVarcharValue(name);
  auto val1 = type::ValueFactory::GetVarcharValue(value, nullptr);
  auto val2 = type::ValueFactory::GetVarcharValue(TypeIdToString(value_type), nullptr);
  auto val3 = type::ValueFactory::GetVarcharValue(description, nullptr);
  auto val4 = type::ValueFactory::GetVarcharValue(min_value, nullptr);
  auto val5 = type::ValueFactory::GetVarcharValue(max_value, nullptr);
  auto val6 = type::ValueFactory::GetVarcharValue(default_value, nullptr);
  auto val7 = type::ValueFactory::GetBooleanValue(is_mutable);
  auto val8 = type::ValueFactory::GetBooleanValue(is_persistent);

  tuple->SetValue(ColumnId::NAME, val0, pool);
  tuple->SetValue(ColumnId::VALUE, val1, pool);
  tuple->SetValue(ColumnId::VALUE_TYPE, val2, pool);
  tuple->SetValue(ColumnId::DESCRIPTION, val3, pool);
  tuple->SetValue(ColumnId::MIN_VALUE, val4, pool);
  tuple->SetValue(ColumnId::MAX_VALUE, val5, pool);
  tuple->SetValue(ColumnId::DEFAULT_VALUE, val6, pool);
  tuple->SetValue(ColumnId::IS_MUTABLE, val7, pool);
  tuple->SetValue(ColumnId::IS_PERSISTENT, val8, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool ConfigCatalog::DeleteConfig(const std::string &name,
                                 concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of config_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::string ConfigCatalog::GetConfigValue(const std::string &name,
                                          concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::VALUE});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
          GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string config_value = "";
  PL_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      config_value = (*result_tiles)[0]->GetValue(0, 0).ToString();
    }
  }
  return config_value;
}

type::TypeId ConfigCatalog::GetConfigType(const std::string &name,
                                          concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::VALUE_TYPE});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
          GetResultWithIndexScan(column_ids, index_offset, values, txn);

  type::TypeId config_type = type::TypeId::INVALID;
  PL_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      config_type = (*result_tiles)[0]->GetValue(0, 0).GetAs<type::TypeId>();
    }
  }
  return config_type;
}

std::string ConfigCatalog::GetMinValue(const std::string &name,
                                       concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::MIN_VALUE});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
          GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string min_value = "";
  PL_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      min_value = (*result_tiles)[0]->GetValue(0, 0).ToString();
    }
  }
  return min_value;
}

std::string ConfigCatalog::GetMaxValue(const std::string &name,
                                          concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::MAX_VALUE});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
          GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string max_value = "";
  PL_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      max_value = (*result_tiles)[0]->GetValue(0, 0).ToString();
    }
  }
  return max_value;
}

std::string ConfigCatalog::GetDefaultValue(const std::string &name,
                                          concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::DEFAULT_VALUE});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
          GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string default_value = "";
  PL_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      default_value = (*result_tiles)[0]->GetValue(0, 0).ToString();
    }
  }
  return default_value;
}

bool ConfigCatalog::IsMutable(const std::string &name,
                              concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::IS_MUTABLE});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
          GetResultWithIndexScan(column_ids, index_offset, values, txn);

  bool is_mutable = false;
  PL_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      is_mutable = (*result_tiles)[0]->GetValue(0, 0).GetAs<bool>();
    }
  }
  return is_mutable;
}
bool ConfigCatalog::IsPersistent(const std::string &name,
                                 concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::IS_PERSISTENT});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
          GetResultWithIndexScan(column_ids, index_offset, values, txn);

  bool is_persistent = false;
  PL_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      is_persistent = (*result_tiles)[0]->GetValue(0, 0).GetAs<bool>();
    }
  }
  return is_persistent;
}

}  // End catalog namespace
}  // End peloton namespace
