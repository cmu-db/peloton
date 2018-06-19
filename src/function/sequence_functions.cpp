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

/*
 * @brief   The actual implementation to get the incremented value for the specified sequence
 * @param  sequence name
 * @param  executor context
 * @return  the next value for the sequence
 * @exception the sequence does not exist
 */
uint32_t SequenceFunctions::Nextval(executor::ExecutorContext &ctx,
                                   const char *sequence_name) {
  PELOTON_ASSERT(sequence_name != nullptr);
  concurrency::TransactionContext* txn = ctx.GetTransaction();

  // HACK: Assume that there is only one database in our cache
  auto all_databases = txn->GetCatalogCache()->GetAllDatabaseObjects();
  PELOTON_ASSERT(all_databases.empty() == false);
  auto database_catalog = all_databases[0];
  LOG_DEBUG("Get database oid: %u", database_catalog->GetDatabaseOid());

  auto sequence_catalog = catalog::Catalog::GetInstance()
          ->GetSystemCatalogs(database_catalog->GetDatabaseOid())
          ->GetSequenceCatalog();

  // initialize a new transaction for incrementing sequence value
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto mini_txn = txn_manager.BeginTransaction();

  // evict the old cached copy of sequence
//  txn->GetCatalogCache()->EvictSequenceObject(sequence_name,
//                                              database_catalog->GetDatabaseOid());
  auto sequence_object = sequence_catalog
          ->GetSequence(database_catalog->GetDatabaseOid(), sequence_name, mini_txn);

  if (sequence_object != nullptr) {
    int64_t val = sequence_object->GetNextVal();
    int64_t curr_val = sequence_object->GetCurrVal();

    // insert the new copy of sequence into cache for future currval
    txn->GetCatalogCache()->InsertSequenceObject(sequence_object);

    auto ret = txn_manager.CommitTransaction(mini_txn);
    if (ret != ResultType::SUCCESS) {
      txn_manager.AbortTransaction(mini_txn);
      return Nextval(ctx, sequence_name);
    }

    catalog::Catalog::GetInstance()
                  ->GetSystemCatalogs(database_catalog->GetDatabaseOid())
                  ->GetSequenceCatalog()
                  ->InsertCurrValCache("FIXME_SCHEMA",
                                       sequence_name, curr_val);
    return val;
  } else {
    throw SequenceException(
            StringUtil::Format("Sequence \"%s\" does not exist", sequence_name));
  }
}

/*
 * @brief   The actual implementation to get the current value for the specified sequence
 * @param  sequence name
 * @param  executor context
 * @return  the current value of a sequence
 * @exception either the sequence does not exist, or 'call nextval before currval'
 */
uint32_t SequenceFunctions::Currval(executor::ExecutorContext &ctx,
                                   const char *sequence_name) {
  PELOTON_ASSERT(sequence_name != nullptr);
  concurrency::TransactionContext* txn = ctx.GetTransaction();
  // get the database oid for this transaction
  // HACK: Assume that there is only one database in our cache
  auto all_databases = txn->GetCatalogCache()->GetAllDatabaseObjects();
  PELOTON_ASSERT(all_databases.empty() == false);
  auto database_catalog = all_databases[0];

  // get the sequence copy from cache
  auto sequence_catalog = catalog::Catalog::GetInstance()
                    ->GetSystemCatalogs(database_catalog->GetDatabaseOid())
                    ->GetSequenceCatalog();

  if(sequence_catalog->CheckCachedCurrValExistence(
      "FIXME_SCHEMA", std::string(sequence_name))) {
    return sequence_catalog->GetCachedCurrVal(
      "FIXME_SCHEMA", std::string(sequence_name));
  } else {
    // get sequence from catalog
    auto sequence_object = sequence_catalog
                    ->GetSequence(database_catalog->GetDatabaseOid(), sequence_name, txn);
    if (sequence_object != nullptr) {
      throw SequenceException(
              StringUtil::Format("currval for sequence \"%s\" is undefined for this session",
                      sequence_name));
    } else {
      // sequence does not exist
      throw SequenceException(
              StringUtil::Format("Sequence \"%s\" does not exist", sequence_name));
    }
  }
}

/*
 * @brief   The wrapper function to get the incremented value for the specified sequence
 * @param  sequence name
 * @param  executor context
 * @return  the result of executing NextVal
 */
type::Value SequenceFunctions::_Nextval(const std::vector<type::Value> &args) {
  executor::ExecutorContext* ctx = (executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  uint32_t ret = SequenceFunctions::Nextval(*ctx, args[0].GetAs<const char *>());
  return type::ValueFactory::GetIntegerValue(ret);
}

/*
 * @brief   The wrapper function to get the current value for the specified sequence
 * @param  sequence name
 * @param  executor context
 * @return  the result of executing CurrVal
 */
type::Value SequenceFunctions::_Currval(const std::vector<type::Value> &args) {
  executor::ExecutorContext* ctx = (executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  uint32_t ret = SequenceFunctions::Currval(*ctx, args[0].GetAs<const char *>());
  return type::ValueFactory::GetIntegerValue(ret);
}

}  // namespace function
}  // namespace peloton
