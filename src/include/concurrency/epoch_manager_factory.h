//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_manager.h
//
// Identification: src/include/concurrency/epoch_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "concurrency/centralized_epoch_manager.h"
#include "concurrency/decentralized_epoch_manager.h"

namespace peloton {
namespace concurrency {

class EpochManagerFactory {
 public:
  static EpochManager& GetInstance() {
    switch (epoch_) {

      case EpochType::CENTRALIZED_EPOCH:
        return CentralizedEpochManager::GetInstance();

      case EpochType::DECENTRALIZED_EPOCH:
        return DecentralizedEpochManager::GetInstance();

      default:
        return CentralizedEpochManager::GetInstance();

    }

  }

  static void Configure(EpochType epoch) {
    epoch_ = epoch;
  }

 private:
  static EpochType epoch_;

};

}
}
