//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mock_executor.h
//
// Identification: test/include/executor/mock_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "gmock/gmock.h"

#include "executor/abstract_executor.h"

namespace peloton {

namespace executor {
class LogicalTile;
}

namespace test {

class MockExecutor : public executor::AbstractExecutor {
 public:
  MockExecutor() : executor::AbstractExecutor(nullptr, nullptr) {}

  MOCK_METHOD0(DInit, bool());

  MOCK_METHOD0(DExecute, bool());

  MOCK_METHOD0(GetOutput, executor::LogicalTile *());
};

}  // namespace test
}  // namespace peloton
