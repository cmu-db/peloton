//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// proc_catalog.cpp
//
// Identification: src/catalog/proc_catalog.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/proc_catalog.h"

#include "catalog/catalog.h"
#include "catalog/language_catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

#define PROC_CATALOG_NAME "pg_proc"

ProcCatalogObject::ProcCatalogObject(executor::LogicalTile *tile,
                                     concurrency::TransactionContext *txn)
    : oid_(tile->GetValue(0, 0).GetAs<oid_t>()),
      name_(tile->GetValue(0, 1).GetAs<const char *>()),
      ret_type_(tile->GetValue(0, 2).GetAs<type::TypeId>()),
      arg_types_(StringToTypeArray(tile->GetValue(0, 3).GetAs<const char *>())),
      lang_oid_(tile->GetValue(0, 4).GetAs<oid_t>()),
      src_(tile->GetValue(0, 5).GetAs<const char *>()),
      txn_(txn) {}

std::unique_ptr<LanguageCatalogObject> ProcCatalogObject::GetLanguage() const {
  return LanguageCatalog::GetInstance().GetLanguageByOid(GetLangOid(), txn_);
}

ProcCatalog &ProcCatalog::GetInstance(concurrency::TransactionContext *txn) {
  static ProcCatalog proc_catalog{txn};
  return proc_catalog;
}

ProcCatalog::~ProcCatalog(){};

ProcCatalog::ProcCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." PROC_CATALOG_NAME
                      " ("
                      "proc_oid      INT NOT NULL PRIMARY KEY, "
                      "proname       VARCHAR NOT NULL, "
                      "prorettype    INT NOT NULL, "
                      "proargtypes   VARCHAR NOT NULL, "
                      "prolang       INT NOT NULL, "
                      "prosrc        VARCHAR NOT NULL);",
                      txn) {
  Catalog::GetInstance()->CreateIndex(CATALOG_DATABASE_NAME, PROC_CATALOG_NAME,
                                      {1, 3}, PROC_CATALOG_NAME "_skey0", false,
                                      IndexType::BWTREE, txn);
}

bool ProcCatalog::InsertProc(const std::string &proname,
                             type::TypeId prorettype,
                             const std::vector<type::TypeId> &proargtypes,
                             oid_t prolang, const std::string &prosrc,
                             type::AbstractPool *pool,
                             concurrency::TransactionContext *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  oid_t proc_oid = GetNextOid();
  auto val0 = type::ValueFactory::GetIntegerValue(proc_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(proname);
  auto val2 = type::ValueFactory::GetIntegerValue(static_cast<int>(prorettype));
  auto val3 =
      type::ValueFactory::GetVarcharValue(TypeIdArrayToString(proargtypes));
  auto val4 = type::ValueFactory::GetIntegerValue(prolang);
  auto val5 = type::ValueFactory::GetVarcharValue(prosrc);

  tuple->SetValue(ColumnId::OID, val0, pool);
  tuple->SetValue(ColumnId::PRONAME, val1, pool);
  tuple->SetValue(ColumnId::PRORETTYPE, val2, pool);
  tuple->SetValue(ColumnId::PROARGTYPES, val3, pool);
  tuple->SetValue(ColumnId::PROLANG, val4, pool);
  tuple->SetValue(ColumnId::PROSRC, val5, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

std::unique_ptr<ProcCatalogObject> ProcCatalog::GetProcByOid(
    oid_t proc_oid, concurrency::TransactionContext *txn) const {
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(proc_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  PL_ASSERT(result_tiles->size() <= 1);

  std::unique_ptr<ProcCatalogObject> ret;
  if (result_tiles->size() == 1) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    ret.reset(new ProcCatalogObject((*result_tiles)[0].get(), txn));
  }

  return ret;
}

std::unique_ptr<ProcCatalogObject> ProcCatalog::GetProcByName(
    const std::string &proc_name,
    const std::vector<type::TypeId> &proc_arg_types,
    concurrency::TransactionContext *txn) const {
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(proc_name).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(
      TypeIdArrayToString(proc_arg_types)).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  PL_ASSERT(result_tiles->size() <= 1);

  std::unique_ptr<ProcCatalogObject> ret;
  if (result_tiles->size() == 1) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    ret.reset(new ProcCatalogObject((*result_tiles)[0].get(), txn));
  }

  return ret;
}

}  // namespace catalog
}  // namespace peloton
