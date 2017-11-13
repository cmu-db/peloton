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

#include "codegen/multi_thread/executor_thread_pool.h"
#include "codegen/codegen.h"
#include "codegen/function_builder.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "common/harness.h"

#include <condition_variable>

namespace peloton {
namespace test {

class ExecutorThreadPoolTest : public PelotonTest {};

// This test case shows how to use ExecutorThreadPool.
TEST_F(ExecutorThreadPoolTest, UseThreadPoolTest) {
  struct TestRuntimeState {
    int ans = 0;
    bool finished = false;
    std::mutex mutex;
    std::condition_variable cv;
  };
  TestRuntimeState runtime_state;

  auto pool = codegen::ExecutorThreadPool::GetInstance();
  pool->SubmitTask(
      reinterpret_cast<char *>(&runtime_state),
      nullptr,
      [](char *ptr, codegen::TaskInfo *task_info) {
        auto test_runtime_state = reinterpret_cast<TestRuntimeState *>(ptr);

        std::unique_lock<std::mutex> lock(test_runtime_state->mutex);
        test_runtime_state->ans = 1;
        test_runtime_state->finished = true;
        lock.unlock();
        test_runtime_state->cv.notify_one();
      }
  );

  // Main thread: wait for finished flag.
  std::unique_lock<std::mutex> lock(runtime_state.mutex);
  runtime_state.cv.wait(lock, [&] {
    return runtime_state.finished;
  });

  EXPECT_EQ(runtime_state.ans, 1);
}

}  // namespace test
}  // namespace peloton
