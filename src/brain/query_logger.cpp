//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_logger.cpp
//
// Identification: src/brain/query_logger.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/query_logger.h"

#include "catalog/query_history_catalog.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace brain {

QueryLogger::Fingerprint::Fingerprint(const std::string &query)
    : query_(query),
      fingerprint_(""),
      fingerprint_result_(pg_query_fingerprint(query.c_str())) {
  if (fingerprint_result_.hexdigest != nullptr) {
    fingerprint_ = fingerprint_result_.hexdigest;
  }
}

QueryLogger::Fingerprint::~Fingerprint() {
  pg_query_free_fingerprint_result(fingerprint_result_);
}

void QueryLogger::LogQuery(std::string query_string, uint64_t timestamp) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  // Perform fingerprint
  Fingerprint fingerprint{query_string};

  // Log query + fingerprint
  auto &query_history_catalog = catalog::QueryHistoryCatalog::GetInstance();
  query_history_catalog.InsertQueryHistory(
      query_string, fingerprint.GetFingerprint(), timestamp, nullptr, txn);

  // We're done
  txn_manager.CommitTransaction(txn);
}

}  // namespace brain
}  // namespace peloton
