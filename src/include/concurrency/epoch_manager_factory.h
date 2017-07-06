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

#include "concurrency/decentralized_epoch_manager.h"

namespace peloton {
namespace concurrency {

class EpochManagerFactory {
 public:
  static EpochManager& GetInstance() {
    switch (epoch_) {
      case EpochType::DECENTRALIZED_EPOCH:
        return DecentralizedEpochManager::GetInstance();

      default:
        return DecentralizedEpochManager::GetInstance();
    }
  }

  static void Configure(EpochType epoch) { epoch_ = epoch; }

  static EpochType GetEpochType() { return epoch_; }

 private:
  static EpochType epoch_;
};
}
}
