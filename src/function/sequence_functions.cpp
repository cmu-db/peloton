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

uint32_t SequenceFunctions::Nextval(executor::ExecutorContext &ctx,
                                   const char *sequence_name) {
  PELOTON_ASSERT(sequence_name != nullptr);
  concurrency::TransactionContext* txn = ctx.GetTransaction();

  // HACK: Assume that there is only one database in our cache
  auto all_databases = txn->GetCatalogCache()->GetAllDatabaseObjects();
  PELOTON_ASSERT(all_databases.empty() == false);
  auto database_catalog = all_databases[0];
  oid_t database_oid = database_catalog->GetDatabaseOid();
  oid_t namespace_oid = INVALID_OID; // FIXME (session namespace)

  auto sequence_catalog = catalog::Catalog::GetInstance()
          ->GetSystemCatalogs(database_oid)
          ->GetSequenceCatalog();

  // initialize a new transaction for incrementing sequence value
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto mini_txn = txn_manager.BeginTransaction();

  // evict the old cached copy of sequence
//  txn->GetCatalogCache()->EvictSequenceObject(sequence_name,
//                                              database_catalog->GetDatabaseOid());
  auto sequence_object =
      sequence_catalog->GetSequence(mini_txn,
                                    database_oid,
                                    namespace_oid,
                                    sequence_name);

  if (sequence_object != nullptr) {
    int64_t val = sequence_object->GetNextVal();
    UNUSED_ATTRIBUTE int64_t curr_val = sequence_object->GetCurrVal();

    // insert the new copy of sequence into cache for future currval
    txn->GetCatalogCache()->InsertSequenceObject(sequence_object);

    auto ret = txn_manager.CommitTransaction(mini_txn);
    if (ret != ResultType::SUCCESS) {
      txn_manager.AbortTransaction(mini_txn);
      return Nextval(ctx, sequence_name);
    }

//    catalog::Catalog::GetInstance()
//                  ->GetSystemCatalogs(database_catalog->GetDatabaseOid())
//                  ->GetSequenceCatalog()
//                  ->InsertCurrValCache("FIXME_SCHEMA",
//                                       sequence_name, curr_val);
    return val;
  } else {
    throw SequenceException(
            StringUtil::Format("Sequence \"%s\" does not exist", sequence_name));
  }
}

uint32_t SequenceFunctions::Currval(executor::ExecutorContext &ctx,
                                   const char *sequence_name) {
  PELOTON_ASSERT(sequence_name != nullptr);
  concurrency::TransactionContext* txn = ctx.GetTransaction();

  // get the database oid for this transaction
  // HACK: Assume that there is only one database in our cache
  auto all_databases = txn->GetCatalogCache()->GetAllDatabaseObjects();
  PELOTON_ASSERT(all_databases.empty() == false);
  auto database_catalog = all_databases[0];
  oid_t database_oid = database_catalog->GetDatabaseOid();
  oid_t namespace_oid = INVALID_OID; // FIXME (session namespace)

  // Check whether we already have a copy of this sequence in our
  // txn's catalog cache. If so, the we always want to return its
  // current value.
  auto sequence_object = txn->GetCatalogCache()
                            ->GetSequenceObject(database_oid,
                                                namespace_oid,
                                                sequence_name);

  // If there was nothing in our CatalogCache, then we need to go
  // get it from the system catalog. This is only to determine whether
  // we want to throw a 'not found' error or not.
  //
  // Important: Even if the sequence exists at this point, we have to
  // throw an exception to say that it is is undefined for this session.
  // Only when they call nextval() on the sequence can they call
  // currval()!
  if (sequence_object == nullptr) {
    auto sequence_catalog = catalog::Catalog::GetInstance()
        ->GetSystemCatalogs(database_catalog->GetDatabaseOid())
        ->GetSequenceCatalog();
    sequence_object =
        sequence_catalog->GetSequence(txn,
                                      database_oid,
                                      namespace_oid,
                                      sequence_name);
    if (sequence_object == nullptr) {
      // sequence does not exist
      throw SequenceException(
          StringUtil::Format("Sequence \"%s\" does not exist", sequence_name));
    } else {
      throw SequenceException(
          StringUtil::Format("currval for sequence \"%s\" is not yet defined in this session",
                             sequence_name));
    }
  }
  PELOTON_ASSERT(sequence_object != nullptr);
  return (sequence_object->GetCurrVal());
}

type::Value SequenceFunctions::_Nextval(const std::vector<type::Value> &args) {
  executor::ExecutorContext* ctx = (executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  uint32_t ret = SequenceFunctions::Nextval(*ctx, args[0].GetAs<const char *>());
  return type::ValueFactory::GetIntegerValue(ret);
}

type::Value SequenceFunctions::_Currval(const std::vector<type::Value> &args) {
  executor::ExecutorContext* ctx = (executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  uint32_t ret = SequenceFunctions::Currval(*ctx, args[0].GetAs<const char *>());
  return type::ValueFactory::GetIntegerValue(ret);
}

}  // namespace function
}  // namespace peloton
