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

#include <sstream>
#include "catalog/catalog.h"
#include "catalog/proc_catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

namespace peloton {
namespace catalog {

ProcCatalog *ProcCatalog::GetInstance(
    concurrency::Transaction *txn) {
  static std::unique_ptr<ProcCatalog> proc_catalog(new ProcCatalog(txn));
  return proc_catalog.get();
}

ProcCatalog::~ProcCatalog() {};

ProcCatalog::ProcCatalog(concurrency::Transaction *txn)
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

  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, PROC_CATALOG_NAME,
      {"proname", "proargtypes"}, PROC_CATALOG_NAME "_skey0",
      false, IndexType::BWTREE, txn);
}

std::string ProcCatalog::TypeArrayToString(
    const std::vector<type::TypeId> types) {
  std::string result = "";
  for (auto type : types) {
    if (result != "")
      result.append(",");
    result.append(TypeIdToString(type));
  }
  return result;
}

std::vector<type::TypeId> ProcCatalog::StringToTypeArray(
    const std::string &types) {
  std::vector<type::TypeId> result;
  std::istringstream stream(types);
  std::string type;
  while (getline(stream, type, ',')) {
    result.push_back(StringToTypeId(type));
  }
  return result;
}

bool ProcCatalog::InsertProc(const std::string &proname,
                             type::TypeId prorettype,
                             const std::vector<type::TypeId> &proargtypes,
                             oid_t prolang,
                             const std::string &prosrc,
                             type::AbstractPool *pool,
                             concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  oid_t proc_oid = GetNextOid();
  auto val0 = type::ValueFactory::GetIntegerValue(proc_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(proname);
  auto val2 = type::ValueFactory::GetIntegerValue(prorettype);
  auto val3 = type::ValueFactory::GetVarcharValue(TypeArrayToString(proargtypes));
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

std::string ProcCatalog::GetProSrc(const std::string &proname,
                                   const std::vector<type::TypeId> &proargtypes,
                                   concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::PROSRC});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(proname).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(
      TypeArrayToString(proargtypes)).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string prosrc;
  PL_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      prosrc = (*result_tiles)[0]->GetValue(0, 0).GetAs<const char*>();
    }
  }
  return prosrc;
}

type::TypeId ProcCatalog::GetProRetType(const std::string &proname,
                                        const std::vector<type::TypeId> &proargtypes,
                                        concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::PRORETTYPE});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(proname).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(
      TypeArrayToString(proargtypes)).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  type::TypeId prorettype = type::TypeId::INVALID;
  PL_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      prorettype = (*result_tiles)[0]->GetValue(0, 0).GetAs<type::TypeId>();
    }
  }
  return prorettype;
}

oid_t ProcCatalog::GetProLang(const std::string &proname,
                              const std::vector<type::TypeId> &proargtypes,
                              concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::PROLANG});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(proname).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(
      TypeArrayToString(proargtypes)).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t prolang = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      prolang = (*result_tiles)[0]->GetValue(0, 0).GetAs<oid_t>();
    }
  }
  return prolang;
}

} // namespace catalog
} // namespace peloton
