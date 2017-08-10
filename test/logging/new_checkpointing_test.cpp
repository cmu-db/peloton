//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// new_checkpointing_test.cpp
//
// Identification: test/logging/new_checkpointing_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/checkpoint_manager_factory.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpointing Tests
//===--------------------------------------------------------------------===//

class NewCheckpointingTests : public PelotonTest {};

TEST_F(NewCheckpointingTests, MyTest) {
  //auto &checkpoint_manager = logging::CheckpointManagerFactory::GetInstance();
  //checkpoint_manager.Reset();
  
  EXPECT_TRUE(true);
}

}
}
