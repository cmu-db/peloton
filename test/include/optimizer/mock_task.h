//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mock_task.h
//
// Identification: test/include/optimizer/mock_task.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "gmock/gmock.h"

#include "optimizer/optimizer_task.h"

namespace peloton {
namespace optimizer {
namespace test {

class MockTask : public optimizer::OptimizerTask {
 public:
  MockTask()
      : optimizer::OptimizerTask(nullptr, OptimizerTaskType::OPTIMIZE_GROUP) {}

  MOCK_METHOD0(execute, void());
};

}  // namespace test
}  // namespace optimizer
}  // namespace peloton
