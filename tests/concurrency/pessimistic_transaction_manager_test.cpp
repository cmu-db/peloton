//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_test.cpp
//
// Identification: tests/concurrency/pessimistic_transaction_manager_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "concurrency/transaction_tests_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class PessimisticTransactionManagerTest : public PelotonTest {};

static std::vector<ConcurrencyType> TEST_TYPES = {
    CONCURRENCY_TYPE_OCC
    // CONCURRENCY_TYPE_2PL
};

}  // End test namespace
}  // End peloton namespace
