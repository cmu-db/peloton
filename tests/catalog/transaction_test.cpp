/*-------------------------------------------------------------------------
*
* tuple_schema_test.cpp
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/test/tuple_schema_test.cpp
*
*-------------------------------------------------------------------------
*/

#include "gtest/gtest.h"

#include "catalog/transaction.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

TEST(TransactionTests, TransactionTest) {

  catalog::TransactionManager *txn_manager = new catalog::TransactionManager();

  catalog::Transaction *txn1, *txn2, *txn3;

  txn1 = txn_manager->BeginTransaction();
  txn2 = txn_manager->BeginTransaction();

  txn_manager->CommitTransaction(txn1);
  txn_manager->EndTransaction(txn1);

  txn3 = txn_manager->BeginTransaction();

  std::cout << (*txn1);
  std::cout << (*txn2);
  std::cout << (*txn3);

  delete txn1;
  delete txn2;
  delete txn3;
}

} // End test namespace
} // End nstore namespace

