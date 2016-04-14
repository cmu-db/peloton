//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// checkpoint_manager.h
//
// Identification: src/backend/logging/checkpoint_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/checkpoint/simple_checkpoint.h"

namespace peloton {
  namespace logging {
    //TODO change to checkpoint manager..
    class CheckpointFactory {
    public:
      static Checkpoint &GetInstance() {
        return SimpleCheckpoint::GetInstance();
      }
    };
  }
}
