//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence_functions.cpp
//
// Identification: src/function/sequence_functions.cpp
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
#include "type/value_factory.h"

namespace peloton {
namespace function {

/*@brief   The actual implementation to get the incremented value for the specified sequence
 * @param   sequence name, executor context
 * @return  the next value for the sequence
 * @exception the sequence does not exist
 */
uint32_t SequenceFunctions::Nextval(executor::ExecutorContext &ctx,
                                   const char *sequence_name) {
  PELOTON_ASSERT(sequence_name != nullptr);
  concurrency::TransactionContext* txn = ctx.GetTransaction();
  // get the database oid for this transaction
  oid_t database_oid = catalog::Catalog::GetInstance()
          ->GetDatabaseObject(ctx.GetDatabaseName(), txn)->GetDatabaseOid();

  // initialize a new transaction for incrementing sequence value
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto mini_txn = txn_manager.BeginTransaction();

  // evict the old cached copy of sequence
  txn->catalog_cache.EvictSequenceObject(sequence_name,database_oid);
  auto sequence_object =
          catalog::Catalog::GetInstance()
                  ->GetSystemCatalogs(database_oid)
                  ->GetSequenceCatalog()
                  ->GetSequence(database_oid, sequence_name, mini_txn);
  if (sequence_object != nullptr) {
    uint32_t val = sequence_object->GetNextVal();
    // insert the new copy of sequence into cache for future currval
    bool insert = txn->catalog_cache.InsertSequenceObject(sequence_object);
    PELOTON_ASSERT(insert);
    auto ret = txn_manager.CommitTransaction(mini_txn);
    while (ret != ResultType::SUCCESS) {
      ret = txn_manager.CommitTransaction(mini_txn);
    }
    return val;
  } else {
    throw SequenceException(
            StringUtil::Format("Sequence \"%s\" does not exist", sequence_name.c_str()));
  }
}

/*@brief   The actual implementation to get the current value for the specified sequence
 * @param   sequence name, executor context
 * @return  the current value of a sequence
 * @exception either the sequence does not exist, or 'call nextval before currval'
 */
uint32_t SequenceFunctions::Currval(executor::ExecutorContext &ctx,
                                   const char *sequence_name) {
  PELOTON_ASSERT(sequence_name != nullptr);
  concurrency::TransactionContext* txn = ctx.GetTransaction();
  // get the database oid for this transaction
  oid_t database_oid = catalog::Catalog::GetInstance()
          ->GetDatabaseObject(ctx.GetDatabaseName(), txn)->GetDatabaseOid();
  // get the sequence copy from cache
  auto sequence_object = txn->
          catalog_cache.GetSequenceObject(sequence_name, database_oid);
  if (sequence_object != nullptr) {
    return sequence_object->GetCurrVal();
  } else {
    throw SequenceException(
            StringUtil::Format("Sequence \"%s\" does not exist", sequence_name.c_str()));
  }
}

/*@brief   The wrapper function to get the incremented value for the specified sequence
 * @param   sequence name, executor context
 * @return  the result of executing NextVal
 */
type::Value SequenceFunctions::_Nextval(const std::vector<type::Value> &args) {
  executor::ExecutorContext* ctx=(executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  uint32_t ret = SequenceFunctions::Nextval(*ctx, args[0].GetAs<const char *>());
  return type::ValueFactory::GetIntegerValue(ret);
}

/*@brief   The wrapper function to get the current value for the specified sequence
 * @param   sequence name, executor context
 * @return  the result of executing CurrVal
 */
type::Value SequenceFunctions::_Currval(const std::vector<type::Value> &args) {
  executor::ExecutorContext* ctx=(executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  uint32_t ret = SequenceFunctions::Currval(*ctx, args[0].GetAs<const char *>());
  return type::ValueFactory::GetIntegerValue(ret);
}

}  // namespace function
}  // namespace peloton
