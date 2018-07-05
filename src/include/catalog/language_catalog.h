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

namespace executor {
class LogicalTile;
}  // namespace executor

namespace catalog {

class LanguageCatalogEntry {
 public:
  LanguageCatalogEntry(executor::LogicalTile *tuple);

  oid_t GetOid() const { return lang_oid_; }

  const std::string &GetName() const { return lang_name_; }

 private:
  oid_t lang_oid_;
  std::string lang_name_;
};

class LanguageCatalog : public AbstractCatalog {
 public:
  ~LanguageCatalog();

  // Global Singleton
  static LanguageCatalog &GetInstance(concurrency::TransactionContext *txn = nullptr);

  oid_t GetNextOid() { return oid_++ | LANGUAGE_OID_MASK; }

  void UpdateOid(oid_t add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertLanguage(concurrency::TransactionContext *txn,
                      const std::string &lanname,
                      type::AbstractPool *pool);

  bool DeleteLanguage(concurrency::TransactionContext *txn,
                      const std::string &lanname);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//

  std::unique_ptr<LanguageCatalogEntry> GetLanguageByOid(concurrency::TransactionContext *txn,
                                                         oid_t lang_oid) const;

  std::unique_ptr<LanguageCatalogEntry> GetLanguageByName(concurrency::TransactionContext *txn,
                                                          const std::string &lang_name) const;

  enum ColumnId {
    OID = 0,
    LANNAME = 1,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids_ = {0, 1};

 private:
  LanguageCatalog(concurrency::TransactionContext *txn);

  enum IndexId {
    PRIMARY_KEY = 0,
    SECONDARY_KEY_0 = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
