//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_thread_pool_test.cpp
//
// Identification: test/codegen/query_thread_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen.h"
#include "codegen/codegen_test_util.h"
#include "codegen/multi_thread_context.h"
#include "codegen/query_thread_pool.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Query Thread Pool Test
//===--------------------------------------------------------------------===//

class QueryThreadPoolTests : public PelotonTest {};

void thread_func(codegen::RuntimeState *, codegen::MultiThreadContext *) {
  // Thread blocks for 100 ms.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  LOG_DEBUG("Thread finish.");
  return;
}

TEST_F(QueryThreadPoolTests, BasicTest) {
  codegen::QueryThreadPool *thread_pool = codegen::QueryThreadPool::GetInstance();

  codegen::CodeContext code_context;
  codegen::CodeGen codegen(code_context);

  codegen::MultiThreadContext *context = new codegen::MultiThreadContext();
  codegen::MultiThreadContext::InitInstance(context, 0, 4);

  codegen::RuntimeState runtime_state;

  thread_pool->SubmitQueryTask(&runtime_state, context, thread_func);
  thread_pool->SubmitQueryTask(&runtime_state, context, thread_func);
  thread_pool->SubmitQueryTask(&runtime_state, context, thread_func);

  LOG_DEBUG("All threads submitted.");
}

}  // End test namespace
}  // End peloton namespace

