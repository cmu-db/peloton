//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_manager_factory.cpp
//
// Identification: src/concurrency/epoch_manager_factory.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//===----------------------------------------------------------------------===//


#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace concurrency {

EpochType EpochManagerFactory::epoch_ = EpochType::DECENTRALIZED_EPOCH;

}
}
