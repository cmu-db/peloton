//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_thread_pool.cpp
//
// Identification: src/codegen/query_thread_pool.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_thread_pool.h"

namespace peloton {
namespace codegen {

QueryThreadPool* QueryThreadPool::GetInstance()
{
    static std::unique_ptr<QueryThreadPool> global_query_thread_pool(new QueryThreadPool());
    return global_query_thread_pool.get();
}

void QueryThreadPool::SubmitQueryTask(RuntimeState *runtime_state, MultiThreadContext *multi_thread_context, void (*target_func)(RuntimeState*,MultiThreadContext*))
{
  pool.SubmitTask(target_func, std::move(runtime_state), std::move(multi_thread_context));
}

}
}
