//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pelton_service.h
//
// Identification: src/backend/networking/pelton_service.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
// this is a hack to get around FATAL conflicting with a google macro

#undef FATAL
#include "backend/networking/logging_service.pb.h"
#include "backend/networking/rpc_server.h"
#include "backend/networking/peloton_endpoint.h"
#define FATAL 21

#include <iostream>

#include "backend/logging/records/tuple_record.h"
#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace logging {

class LoggingService : public networking::PelotonLoggingService {
 public:
  // implements LoggingService ------------------------------------------

  LoggingService();

  virtual void LogRecordReplay(
      ::google::protobuf::RpcController *controller,
      const networking::LogRecordReplayRequest *request,
      networking::LogRecordReplayResponse *response,
      ::google::protobuf::Closure *done);

 private:
  void StartTransactionRecovery(cid_t commit_id);

  void CommitTransactionRecovery(cid_t commit_id);

  void InsertTuple(TupleRecord *recovery_txn,
                   concurrency::TransactionManager &txn_manager);

  void DeleteTuple(TupleRecord *recovery_txn,
                   concurrency::TransactionManager &txn_manager);

  void UpdateTuple(TupleRecord *recovery_txn,
                   concurrency::TransactionManager &txn_manager);
  // Txn table during recovery
  std::map<txn_id_t, std::vector<TupleRecord *>> recovery_txn_table;

  // pool for allocating non-inlined values
  VarlenPool *recovery_pool;

  std::atomic<long> replication_sequence_number_;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_oid = 0;
  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_cid = 0;
};

// void StartPelotonService();

}  // namespace networking
}  // namespace peloton
