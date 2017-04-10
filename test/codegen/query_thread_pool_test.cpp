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

TEST_F(QueryThreadPoolTests, BasicTest) {
  codegen::QueryThreadPool thread_pool(4);

  codegen::CodeContext code_context;
  codegen::CodeGen codegen(code_context);

  std::atomic<bool> finished(false);
  codegen::MultiThreadContext context(codegen.Const64(0), codegen.Const64(4), &finished);

  thread_pool.SubmitQueryTask(context, [](__attribute__ ((unused)) codegen::MultiThreadContext context){
    // Thread blocks for 100 ms.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    context.ThreadFinish();
    return;
  });

  while (finished.load() == false) {
    LOG_DEBUG("Thread not finish, wait for another 10ms...");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  LOG_DEBUG("All threads finish.");
}

}  // End test namespace
}  // End peloton namespace

