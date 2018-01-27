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
#include "parser/pg_query.h"

namespace peloton {
namespace brain {

void QueryLogger::LogQuery(std::string query_string, uint64_t timestamp) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::string fingerprint = pg_query_fingerprint(query_string.c_str()).hexdigest;

  catalog::QueryHistoryCatalog::GetInstance()->InsertQueryHistory(
      query_string, fingerprint, timestamp, nullptr, txn);

  txn_manager.CommitTransaction(txn);
}

}  // namespace brain
}  // namespace peloton
