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

ProcCatalogEntry::ProcCatalogEntry(concurrency::TransactionContext *txn,
                                     executor::LogicalTile *tile)
    : oid_(tile->GetValue(0, 0).GetAs<oid_t>()),
      name_(tile->GetValue(0, 1).GetAs<const char *>()),
      ret_type_(tile->GetValue(0, 2).GetAs<type::TypeId>()),
      arg_types_(StringToTypeArray(tile->GetValue(0, 3).GetAs<const char *>())),
      lang_oid_(tile->GetValue(0, 4).GetAs<oid_t>()),
      src_(tile->GetValue(0, 5).GetAs<const char *>()),
      txn_(txn) {}

std::unique_ptr<LanguageCatalogEntry> ProcCatalogEntry::GetLanguage() const {
  return LanguageCatalog::GetInstance().GetLanguageByOid(txn_, GetLangOid());
}

ProcCatalog &ProcCatalog::GetInstance(concurrency::TransactionContext *txn) {
  static ProcCatalog proc_catalog{txn};
  return proc_catalog;
}

ProcCatalog::~ProcCatalog(){};

ProcCatalog::ProcCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog(txn, "CREATE TABLE " CATALOG_DATABASE_NAME
                           "." CATALOG_SCHEMA_NAME "." PROC_CATALOG_NAME
                           " ("
                           "proc_oid      INT NOT NULL PRIMARY KEY, "
                           "proname       VARCHAR NOT NULL, "
                           "prorettype    INT NOT NULL, "
                           "proargtypes   VARCHAR NOT NULL, "
                           "prolang       INT NOT NULL, "
                           "prosrc        VARCHAR NOT NULL);") {
  Catalog::GetInstance()->CreateIndex(txn,
                                      CATALOG_DATABASE_NAME,
                                      CATALOG_SCHEMA_NAME,
                                      PROC_CATALOG_NAME,
                                      PROC_CATALOG_NAME "_skey0",
                                      {1, 3},
                                      false,
                                      IndexType::BWTREE);
}

bool ProcCatalog::InsertProc(concurrency::TransactionContext *txn,
                             const std::string &proname,
                             type::TypeId prorettype,
                             const std::vector<type::TypeId> &proargtypes,
                             oid_t prolang,
                             const std::string &prosrc,
                             type::AbstractPool *pool) {
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
  return InsertTuple(txn, std::move(tuple));
}

std::unique_ptr<ProcCatalogEntry> ProcCatalog::GetProcByOid(concurrency::TransactionContext *txn,
                                                             oid_t proc_oid) const {
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(proc_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);
  PELOTON_ASSERT(result_tiles->size() <= 1);

  std::unique_ptr<ProcCatalogEntry> ret;
  if (result_tiles->size() == 1) {
    PELOTON_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    ret.reset(new ProcCatalogEntry(txn, (*result_tiles)[0].get()));
  }

  return ret;
}

std::unique_ptr<ProcCatalogEntry> ProcCatalog::GetProcByName(concurrency::TransactionContext *txn,
                                                              const std::string &proc_name,
                                                              const std::vector<type::TypeId> &proc_arg_types) const {
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(proc_name).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(TypeIdArrayToString(proc_arg_types))
          .Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);
  PELOTON_ASSERT(result_tiles->size() <= 1);

  std::unique_ptr<ProcCatalogEntry> ret;
  if (result_tiles->size() == 1) {
    PELOTON_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    ret.reset(new ProcCatalogEntry(txn, (*result_tiles)[0].get()));
  }

  return ret;
}

}  // namespace catalog
}  // namespace peloton
