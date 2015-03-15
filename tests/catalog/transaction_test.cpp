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

  catalog::Transaction *txn = new catalog::Transaction();

  std::cout << (*txn);

  delete txn;
}

} // End test namespace
} // End nstore namespace

