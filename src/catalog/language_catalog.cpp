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

#include "catalog/language_catalog.h"

#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

LanguageCatalogObject::LanguageCatalogObject(executor::LogicalTile *tuple)
    : lang_oid_(tuple->GetValue(0, 0).GetAs<oid_t>()),
      lang_name_(tuple->GetValue(0, 1).GetAs<const char *>()) {}

LanguageCatalog &LanguageCatalog::GetInstance(concurrency::TransactionContext *txn) {
  static LanguageCatalog language_catalog{txn};
  return language_catalog;
}

LanguageCatalog::~LanguageCatalog(){};

LanguageCatalog::LanguageCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." LANGUAGE_CATALOG_NAME
                      " ("
                      "language_oid   INT NOT NULL PRIMARY KEY, "
                      "lanname        VARCHAR NOT NULL);",
                      txn) {
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, LANGUAGE_CATALOG_NAME, {1},
      LANGUAGE_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);
}

// insert a new language by name
bool LanguageCatalog::InsertLanguage(const std::string &lanname,
                                     type::AbstractPool *pool,
                                     concurrency::TransactionContext *txn) {
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
                                     concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::SECONDARY_KEY_0;

  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(lanname, nullptr).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::unique_ptr<LanguageCatalogObject> LanguageCatalog::GetLanguageByOid(
    oid_t lang_oid, concurrency::TransactionContext *txn) const {
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(lang_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  PL_ASSERT(result_tiles->size() <= 1);

  std::unique_ptr<LanguageCatalogObject> ret;
  if (result_tiles->size() == 1) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    ret.reset(new LanguageCatalogObject((*result_tiles)[0].get()));
  }

  return ret;
}

std::unique_ptr<LanguageCatalogObject> LanguageCatalog::GetLanguageByName(
    const std::string &lang_name, concurrency::TransactionContext *txn) const {
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(lang_name).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  PL_ASSERT(result_tiles->size() <= 1);

  std::unique_ptr<LanguageCatalogObject> ret;
  if (result_tiles->size() == 1) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    ret.reset(new LanguageCatalogObject((*result_tiles)[0].get()));
  }

  return ret;
}

}  // namespace catalog
}  // namespace peloton
