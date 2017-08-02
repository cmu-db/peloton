//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// proc_catalog.h
//
// Identification: src/include/catalog/proc_catalog.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


//===----------------------------------------------------------------------===//
// pg_proc
//
// Schema: (column offset: column_name)
// 0: proc_oid (pkey)
// 1: proname (skey_0)
// 2: prorettype
// 3: proargtypes (skey_0)
// 4: prolang
// 5: prosrc
//
// Indexes: (index offset: indexed columns)
// 0: proc_oid (unique & primary key)
// 1: proname & proargtypes (secondary key 0)
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

#define PROC_CATALOG_NAME "pg_proc"

namespace peloton {
namespace catalog {

class ProcCatalog : public AbstractCatalog {
 public:
  ~ProcCatalog();

  // Global Singleton
  static ProcCatalog *GetInstance(
      concurrency::Transaction *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertProc(const std::string &proname,
                  type::TypeId prorettype,
                  const std::vector<type::TypeId> &proargtypes,
                  oid_t prolang,
                  const std::string &prosrc,
                  type::AbstractPool *pool, concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::string GetProSrc(const std::string &proname,
                        const std::vector<type::TypeId> &proargtypes,
                        concurrency::Transaction *txn);

  type::TypeId GetProRetType(const std::string &proname,
                             const std::vector<type::TypeId> &proargtypes,
                             concurrency::Transaction *txn);

  oid_t GetProLang(const std::string &proname,
                   const std::vector<type::TypeId> &proargtypes,
                   concurrency::Transaction *txn);

  enum ColumnId {
    OID = 0,
    PRONAME = 1,
    PRORETTYPE = 2,
    PROARGTYPES = 3,
    PROLANG = 4,
    PROSRC = 5,
    // Add new columns here in creation order
  };

 private:
  ProcCatalog(concurrency::Transaction *txn);

  oid_t GetNextOid() { return oid_++ | PROC_OID_MASK; }

  static std::string TypeArrayToString(const std::vector<type::TypeId> types);

  static std::vector<type::TypeId> StringToTypeArray(const std::string &types);

  enum IndexId {
    PRIMARY_KEY = 0,
    SECONDARY_KEY_0 = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
