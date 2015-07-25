/**
 * @brief Implementation of mock executor.
 *
 * See executor tests for usage.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "gmock/gmock.h"

#include "backend/executor/abstract_executor.h"

namespace peloton {

namespace executor {
class LogicalTile;
}

namespace test {

class MockExecutor : public executor::AbstractExecutor {
 public:
  MockExecutor() : executor::AbstractExecutor(nullptr) {}

  MOCK_METHOD0(DInit, bool());

  MOCK_METHOD0(DExecute, bool());

  MOCK_METHOD0(GetOutput, executor::LogicalTile*());
};

}  // namespace test
}  // namespace peloton
