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
    int thread_count = std::thread::hardware_concurrency() + 3;
    static std::unique_ptr<QueryThreadPool> global_query_thread_pool(new QueryThreadPool(thread_count));
    return global_query_thread_pool.get();
}

// TODO: runtime_state and multi_thread_context could be invalid after task being submitted!
// we might need unique_ptr here!
void QueryThreadPool::SubmitQueryTask(RuntimeState *runtime_state, MultiThreadContext *multi_thread_context, void (*target_func)(RuntimeState*,MultiThreadContext*))
{
    pool.SubmitTask(target_func, std::move(runtime_state), std::move(multi_thread_context));
}

}
}
