//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions.cpp
//
// Identification: src/function/string_functions.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/sequence_functions.h"

#include "common/macros.h"
#include "executor/executor_context.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/sequence_catalog.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace function {

type::Value OldEngineStringFunctions::Nextval(
        UNUSED_ATTRIBUTE const std::vector<type::Value> &args) {
  executor::ExecutorContext* ctx=(executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  concurrency::TransactionContext* txn = ctx->GetTransaction();
  const char * sequence_name = args[0].GetAs<const char *>();
  PELOTON_ASSERT(sequence_name != nullptr);
  oid_t database_oid = catalog::Catalog::GetInstance()
          ->GetDatabaseObject(ctx->GetDatabaseName(), txn)->GetDatabaseOid();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto mini_txn = txn_manager.BeginTransaction();
  txn->catalog_cache.EvictSequenceObject(sequence_name,database_oid);
  auto sequence_object =
          catalog::Catalog::GetInstance()
                  ->GetSystemCatalogs(database_oid)
                  ->GetSequenceCatalog()
                  ->GetSequence(database_oid, sequence_name, mini_txn);
  if (sequence_object != nullptr) {
    int64_t val = sequence_object->GetNextVal();
    bool insert = txn->catalog_cache.InsertSequenceObject(sequence_object);
    PELOTON_ASSERT(insert);
    txn_manager.CommitTransaction(mini_txn);
    return type::ValueFactory::GetIntegerValue(val);
  } else {
    throw SequenceException(
            StringUtil::Format("relation \"%s\" does not exist", sequence_name));
  }
}

type::Value OldEngineStringFunctions::Currval(
        UNUSED_ATTRIBUTE const std::vector<type::Value> &args) {
  executor::ExecutorContext* ctx=(executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  concurrency::TransactionContext* txn = ctx->GetTransaction();
  const char * sequence_name = args[0].GetAs<const char *>();
  PELOTON_ASSERT(sequence_name != nullptr);
  oid_t database_oid = catalog::Catalog::GetInstance()
          ->GetDatabaseObject(ctx->GetDatabaseName(), txn)->GetDatabaseOid();
  auto sequence_object = txn->catalog_cache.GetSequenceObject(sequence_name,
                                                              database_oid);
  if (sequence_object != nullptr) {
    return type::ValueFactory::GetIntegerValue(sequence_object->GetCurrVal());
  } else {
    throw SequenceException(
            StringUtil::Format("relation \"%s\" does not exist", sequence_name));
  }
}

}  // namespace function
}  // namespace peloton
