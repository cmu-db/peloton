//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// language_catalog.h
//
// Identification: src/include/catalog/language_catalog.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_language
//
// Schema: (column offset: column_name)
// 0: language_oid (pkey)
// 1: lanname (skey_0)
//
// Indexes: (index offset: indexed columns)
// 0: language_oid (unique & primary key)
// 1: lanname (secondary key 0)
//===----------------------------------------------------------------------===//
#pragma once

#include "catalog/abstract_catalog.h"

#define LANGUAGE_CATALOG_NAME "pg_language"

namespace peloton {
namespace catalog {

class LanguageCatalog : public AbstractCatalog {
 public:
  ~LanguageCatalog();

  // Global Singleton
  static LanguageCatalog &GetInstance(concurrency::Transaction *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertLanguage(const std::string &lanname, type::AbstractPool *pool,
                      concurrency::Transaction *txn);

  bool DeleteLanguage(const std::string &lanname,
                      concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  oid_t GetLanguageOid(const std::string &lanname,
                       concurrency::Transaction *txn);

  std::string GetLanguageName(oid_t language_oid,
                              concurrency::Transaction *txn);

  enum ColumnId {
    OID = 0,
    LANNAME = 1,
    // Add new columns here in creation order
  };

 private:
  LanguageCatalog(concurrency::Transaction *txn);

  oid_t GetNextOid() { return oid_++ | LANGUAGE_OID_MASK; }

  enum IndexId {
    PRIMARY_KEY = 0,
    SECONDARY_KEY_0 = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
