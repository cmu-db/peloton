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

class LanguageCatalogObject {
 public:
  LanguageCatalogObject(executor::LogicalTile *tuple);

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

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertLanguage(const std::string &lanname, type::AbstractPool *pool,
                      concurrency::TransactionContext *txn);

  bool DeleteLanguage(const std::string &lanname,
                      concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//

  std::unique_ptr<LanguageCatalogObject> GetLanguageByOid(
      oid_t lang_oid, concurrency::TransactionContext *txn) const;

  std::unique_ptr<LanguageCatalogObject> GetLanguageByName(
      const std::string &lang_name, concurrency::TransactionContext *txn) const;

  enum ColumnId {
    OID = 0,
    LANNAME = 1,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1};

 private:
  LanguageCatalog(concurrency::TransactionContext *txn);

  oid_t GetNextOid() { return oid_++ | LANGUAGE_OID_MASK; }

  enum IndexId {
    PRIMARY_KEY = 0,
    SECONDARY_KEY_0 = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
