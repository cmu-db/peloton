//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// durability_factory.cpp
//
// Identification: src/backend/logging/durability_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/durability_factory.h"
#include "concurrency/epoch_manager_factory.h"

namespace peloton {
namespace logging {

  LoggingType DurabilityFactory::logging_type_ = LoggingType::INVALID;

  CheckpointType DurabilityFactory::checkpoint_type_ = CHECKPOINT_TYPE_INVALID;

  TimerType DurabilityFactory::timer_type_ = TIMER_OFF;

  bool DurabilityFactory::generate_detailed_csv_ = false;


  //////////////////////////////////

}
}
