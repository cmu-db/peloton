//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// checkpoint_test.cpp
//
// Identification: tests/planner/checkpoint_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "backend/logging/checkpoint.h"
#include "backend/logging/checkpoint/simple_checkpoint.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Planner Tests
//===--------------------------------------------------------------------===//

class CheckpointTests : public PelotonTest {};

TEST_F(CheckpointTests, BasicTest) {
  // logging::Checkpoint &checkpoint =
  // logging::CheckpointFactory::GetInstance();
}

}  // End test namespace
}  // End peloton namespace
