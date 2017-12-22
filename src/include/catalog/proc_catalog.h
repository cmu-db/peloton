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

namespace peloton {
namespace catalog {

// Forward declare
class LanguageCatalogObject;

//===----------------------------------------------------------------------===//
// In-memory representation of a row from the pg_proc table.
//===----------------------------------------------------------------------===//
class ProcCatalogObject {
 public:
  ProcCatalogObject(executor::LogicalTile *tile, concurrency::TransactionContext *txn);

  // Accessors

  oid_t GetOid() const { return oid_; }

  const std::string &GetName() const { return name_; }

  type::TypeId GetRetType() const { return ret_type_; }

  const std::vector<type::TypeId> &GetArgTypes() const { return arg_types_; }

  oid_t GetLangOid() const { return lang_oid_; }

  std::unique_ptr<LanguageCatalogObject> GetLanguage() const;

  const std::string &GetSrc() const { return src_; }

 private:
  // The fields of 'pg_proc' table
  oid_t oid_;
  std::string name_;
  type::TypeId ret_type_;
  const std::vector<type::TypeId> arg_types_;
  oid_t lang_oid_;
  std::string src_;

  // Only valid in this transaction
  concurrency::TransactionContext *txn_;
};

//===----------------------------------------------------------------------===//
// The pg_proc catalog table.
//===----------------------------------------------------------------------===//
class ProcCatalog : public AbstractCatalog {
 public:
  ~ProcCatalog();

  // Global Singleton
  static ProcCatalog &GetInstance(concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertProc(const std::string &proname, type::TypeId prorettype,
                  const std::vector<type::TypeId> &proargtypes, oid_t prolang,
                  const std::string &prosrc, type::AbstractPool *pool,
                  concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//

  std::unique_ptr<ProcCatalogObject> GetProcByOid(
      oid_t proc_oid, concurrency::TransactionContext *txn) const;

  std::unique_ptr<ProcCatalogObject> GetProcByName(
      const std::string &proc_name,
      const std::vector<type::TypeId> &proc_arg_types,
      concurrency::TransactionContext *txn) const;

  enum ColumnId {
    OID = 0,
    PRONAME = 1,
    PRORETTYPE = 2,
    PROARGTYPES = 3,
    PROLANG = 4,
    PROSRC = 5,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1, 2, 3, 4, 5};

 private:
  ProcCatalog(concurrency::TransactionContext *txn);

  oid_t GetNextOid() { return oid_++ | PROC_OID_MASK; }

  enum IndexId {
    PRIMARY_KEY = 0,
    SECONDARY_KEY_0 = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
