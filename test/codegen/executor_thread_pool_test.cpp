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

#include <tuple>
#include "codegen/function_builder.h"
#include "codegen/multi_thread/executor_thread_pool.h"
#include "codegen/multi_thread/count_down.h"
#include "codegen/proxy/executor_thread_pool_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/count_down_proxy.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class ExecutorThreadPoolTest : public PelotonTest {
 public:
  struct TestRuntimeState {
    int ans = 0;

    // This is what codegen will generate.
    char count_down[sizeof(codegen::CountDown)] = {0};
  };

  codegen::CountDown *GetCountDown(TestRuntimeState *runtime_state) {
    return reinterpret_cast<codegen::CountDown *>(&runtime_state->count_down);
  }

  void InitTestRuntimeState(TestRuntimeState *runtime_state) {
    auto count_down = GetCountDown(runtime_state);
    count_down->Init(1);
  }

  std::tuple<codegen::RuntimeState::StateID, codegen::RuntimeState::StateID>
  SetupRuntimeState(codegen::CodeGen &cgen,
                    codegen::RuntimeState &runtime_state) {
    auto count_down_type = codegen::CountDownProxy::GetType(cgen);

    auto ans_state_id = runtime_state.RegisterState("ans", cgen.Int32Type());
    auto count_down_state_id = runtime_state.RegisterState(
        "count_down", count_down_type);

    runtime_state.FinalizeType(cgen);

    return std::make_tuple(ans_state_id, count_down_state_id);
  }

  llvm::Function *BuildTaskFunc(
      codegen::CodeGen &cgen,
      codegen::RuntimeState &runtime_state,
      codegen::RuntimeState::StateID ans_id,
      codegen::RuntimeState::StateID count_down_id) {
    auto &code_context = cgen.GetCodeContext();
    auto runtime_state_type = runtime_state.FinalizeType(cgen);
    auto task_info_type = codegen::TaskInfoProxy::GetType(cgen);

    // void task(RuntimeState *runtime_state, TaskInfo *task_info) {
    //   runtime_state->ans = 1;
    //   runtime_state->count_down.Wait();
    // }
    codegen::FunctionBuilder task(code_context, "task", cgen.VoidType(), {
        {"runtime_state", runtime_state_type->getPointerTo()},
        {"task_info", task_info_type->getPointerTo()}
    });
    {
      auto ans_ptr = runtime_state.LoadStatePtr(cgen, ans_id);
      cgen->CreateStore(cgen.Const32(1), ans_ptr);

      auto count_down_ptr = runtime_state.LoadStatePtr(cgen, count_down_id);
      cgen.Call(codegen::CountDownProxy::Decrease, {count_down_ptr});

      task.ReturnAndFinish();
    }

    return task.GetFunction();
  }
};

// This test case shows how to use ExecutorThreadPool.
TEST_F(ExecutorThreadPoolTest, UseThreadPoolTest) {
  TestRuntimeState test_runtime_state;
  InitTestRuntimeState(&test_runtime_state);
  auto count_down = GetCountDown(&test_runtime_state);

  auto pool = codegen::ExecutorThreadPool::GetInstance();
  pool->SubmitTask(
      reinterpret_cast<char *>(&test_runtime_state),
      nullptr,
      [](char *ptr, UNUSED_ATTRIBUTE codegen::TaskInfo *task_info) {
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

TEST_F(ExecutorThreadPoolTest, CodeGenTaskTest) {
  codegen::CodeContext code_context;
  codegen::CodeGen cgen(code_context);

  // Create the runtime state type in module.
  codegen::RuntimeState runtime_state;
  codegen::RuntimeState::StateID ans_id, count_down_id;
  std::tie(ans_id, count_down_id) =
      SetupRuntimeState(cgen, runtime_state);

  // Build task function.
  auto task_func = BuildTaskFunc(cgen, runtime_state, ans_id, count_down_id);

  // Compile the module.
  EXPECT_TRUE(code_context.Compile());

  // Prepare runtime state.
  TestRuntimeState test_runtime_state;
  InitTestRuntimeState(&test_runtime_state);

  // Submit task.
  auto pool = codegen::ExecutorThreadPool::GetInstance();
  auto fn = reinterpret_cast<codegen::ExecutorThreadPool::func_t>(
      code_context.GetRawFunctionPointer(task_func)
  );
  pool->SubmitTask(reinterpret_cast<char *>(&test_runtime_state), nullptr, fn);
  auto count_down = GetCountDown(&test_runtime_state);
  count_down->Wait();

  EXPECT_EQ(test_runtime_state.ans, 1);

  count_down->Destroy();
}

// This test case shows how to generate code that uses ExecutorThreadPool.
TEST_F(ExecutorThreadPoolTest, CodeGenThreadPoolTest) {
  codegen::CodeContext code_context;
  codegen::CodeGen cgen(code_context);
  auto task_info_type = codegen::TaskInfoProxy::GetType(cgen);

  // Create the runtime state type in module.
  // Create the runtime state type in module.
  codegen::RuntimeState runtime_state;
  codegen::RuntimeState::StateID ans_id, count_down_id;
  std::tie(ans_id, count_down_id) =
      SetupRuntimeState(cgen, runtime_state);

  // Build task function.
  auto task_func = BuildTaskFunc(cgen, runtime_state, ans_id, count_down_id);

  // void func(RuntimeState *runtime_state);
  codegen::FunctionBuilder func(code_context, "func", cgen.VoidType(), {
      {"runtime_state", runtime_state.FinalizeType(cgen)->getPointerTo()}
  });
  {
    // auto count_down = runtime_state->count_down;
    auto count_down_ptr = runtime_state.LoadStatePtr(cgen, count_down_id);

    // count_down->Init(1);
    cgen.Call(codegen::CountDownProxy::Init, {count_down_ptr, cgen.Const32(1)});

    // auto thread_pool = codegen::ExecutorThreadPool::GetInstance();
    auto thread_pool_ptr = cgen.Call(
        codegen::ExecutorThreadPoolProxy::GetInstance, {}
    );

    // using task_type = void (*)(char *ptr, TaskInfo *);
    auto task_type = llvm::FunctionType::get(
        cgen.VoidType(), {
            cgen.CharPtrType(),
            task_info_type->getPointerTo()
        },
        false
    )->getPointerTo();

    // thread_pool->SubmitTask(
    //   (char *)runtime_state,
    //   (TaskInfo *)NULL,
    //   (task_type)task
    // );
    cgen.Call(codegen::ExecutorThreadPoolProxy::SubmitTask, {
        thread_pool_ptr,
        cgen->CreatePointerCast(cgen.GetState(), cgen.CharPtrType()),
        cgen.NullPtr(task_info_type->getPointerTo()),
        cgen->CreatePointerCast(task_func, task_type)
    });

    // count_down->Wait();
    cgen.Call(codegen::CountDownProxy::Wait, {count_down_ptr});

    // count_down->Destroy();
    cgen.Call(codegen::CountDownProxy::Destroy, {count_down_ptr});

    func.ReturnAndFinish();
  }

  EXPECT_TRUE(code_context.Compile());

  TestRuntimeState test_runtime_state;
  auto fn = reinterpret_cast<void (*)(char *)>(
      code_context.GetRawFunctionPointer(func.GetFunction())
  );
  fn(reinterpret_cast<char *>(&test_runtime_state));

  EXPECT_EQ(test_runtime_state.ans, 1);
}

}  // namespace test
}  // namespace peloton
