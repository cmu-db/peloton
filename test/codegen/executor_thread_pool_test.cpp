//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_pool_test.cpp
//
// Identification: test/codegen/thread_pool_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/function_builder.h"
#include "codegen/multi_thread/executor_thread_pool.h"
#include "codegen/multi_thread/count_down.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class ExecutorThreadPoolTest : public PelotonTest {};

// This test case shows how to use ExecutorThreadPool.
TEST_F(ExecutorThreadPoolTest, UseThreadPoolTest) {
  struct TestRuntimeState {
    int ans = 0;

    // This is what codegen will generate.
    char count_down[sizeof(codegen::CountDown)] = {0};
  };
  TestRuntimeState test_runtime_state;

  auto count_down = reinterpret_cast<codegen::CountDown *>(
      &test_runtime_state.count_down
  );
  count_down->Init(1);

  auto pool = codegen::ExecutorThreadPool::GetInstance();
  pool->SubmitTask(
      reinterpret_cast<char *>(&test_runtime_state),
      nullptr,
      [](char *ptr, codegen::TaskInfo *task_info) {
        auto test_runtime_state = reinterpret_cast<TestRuntimeState *>(ptr);

        test_runtime_state->ans = 1;

        auto count_down = reinterpret_cast<codegen::CountDown *>(
            &test_runtime_state->count_down
        );
        count_down->Decrease();
      }
  );

  count_down->Wait();

  EXPECT_EQ(test_runtime_state.ans, 1);

  count_down->Destroy();
}

}  // namespace test
}  // namespace peloton
