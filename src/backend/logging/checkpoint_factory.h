//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager_factory.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/checkpoint/simple_checkpoint.h"

namespace peloton {
  namespace logging {
    class CheckpointFactory {
    public:
      static Checkpoint &GetInstance() {
        return SimpleCheckpoint::GetInstance();
      }
    };
  }
}
