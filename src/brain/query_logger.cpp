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

void QueryLogger::LogQuery(std::string query_string, uint64_t timestamp) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // TODO[Siva]: call pg_fingerprint() after Marcel's PR is merged
  std::string fingerprint = "fingerprint";

  catalog::QueryHistoryCatalog::GetInstance()->InsertQueryHistory(
      query_string, fingerprint, timestamp, nullptr, txn);

  txn_manager.CommitTransaction(txn);
}

}  // namespace brain
}  // namespace peloton
