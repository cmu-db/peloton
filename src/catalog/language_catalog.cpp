//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// language_catalog.cpp
//
// Identification: src/catalog/language_catalog.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "catalog/catalog.h"
#include "catalog/language_catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"

namespace peloton {
namespace catalog {

LanguageCatalog *LanguageCatalog::GetInstance(
    concurrency::Transaction *txn) {
  static LanguageCatalog language_catalog{txn};
  return &language_catalog;
}

LanguageCatalog::~LanguageCatalog() {};

LanguageCatalog::LanguageCatalog(concurrency::Transaction *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                          "." LANGUAGE_CATALOG_NAME
                          " ("
                          "language_oid   INT NOT NULL PRIMARY KEY, "
                          "lanname        VARCHAR NOT NULL);",
                      txn) {
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, LANGUAGE_CATALOG_NAME,
      {"lanname"}, LANGUAGE_CATALOG_NAME "_skey0",
      false, IndexType::BWTREE, txn);
}

// insert a new language by name
bool LanguageCatalog::InsertLanguage(const std::string &lanname,
                                     type::AbstractPool *pool,
                                     concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  oid_t language_oid = GetNextOid();
  auto val0 = type::ValueFactory::GetIntegerValue(language_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(lanname);

  tuple->SetValue(ColumnId::OID, val0, pool);
  tuple->SetValue(ColumnId::LANNAME, val1, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

// delete a language by name
bool LanguageCatalog::DeleteLanguage(const std::string &lanname,
                                     concurrency::Transaction *txn) {
  oid_t index_offset = IndexId::SECONDARY_KEY_0;

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(lanname, nullptr).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

// get language_oid by name
oid_t LanguageCatalog::GetLanguageOid(const std::string &lanname,
                                      concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::OID});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(lanname).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t language_oid = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      language_oid = (*result_tiles)[0]->GetValue(0, 0).GetAs<oid_t>();
    }
  }
  return language_oid;
}

// get language name by oid
std::string LanguageCatalog::GetLanguageName(oid_t language_oid,
                                             concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::LANNAME});
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(language_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string lanname;
  PL_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      lanname = (*result_tiles)[0]->GetValue(0, 0).GetAs<const char*>();
    }
  }
  return lanname;
}

}  // namespace catalog
}  // namespace peloton
