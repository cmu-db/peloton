//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query.cpp
//
// Identification: src/codegen/query.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/mono_queue_pool.h"
#include "codegen/stage.h"
#include "codegen/query.h"
#include "codegen/query_result_consumer.h"
#include "common/timer.h"
#include "executor/plan_executor.h"
#include "storage/storage_manager.h"

namespace peloton {
namespace codegen {

// Constructor
Query::Query(const planner::AbstractPlan &query_plan)
    : query_plan_(query_plan) {}

static void ExecuteStages(std::vector<Stage>::iterator begin,
                          std::vector<Stage>::iterator end,
                          char *param,
                          std::function<void()> on_complete) {
  if (begin == end) {
    on_complete();
  } else {
    auto &pool = threadpool::MonoQueuePool::GetInstance();
    switch (begin->kind_) {
      case StageKind::SINGLE_THREADED: {
        pool.SubmitTask([&pool, begin, end, param, on_complete] {
          begin->func_ptr_(param);
          ExecuteStages(begin + 1, end, param, on_complete);
        });
        break;
      }
    }
  }
}

void Query::Execute(std::unique_ptr<executor::ExecutorContext> executor_context,
                    QueryResultConsumer &consumer,
                    std::function<void(executor::ExecutionResult)> on_complete,
                    RuntimeStats *stats) {
  executor::ExecutorContext *context = executor_context.release();
  CodeGen codegen{GetCodeContext()};

  llvm::Type *runtime_state_type = runtime_state_.FinalizeType(codegen);
  size_t parameter_size = codegen.SizeOf(runtime_state_type);
  PL_ASSERT((parameter_size % 8 == 0) && "parameter size not multiple of 8");

  // Allocate some space for the function arguments
  char *param = new char[parameter_size];
  PL_MEMSET(param, 0, parameter_size);

  // We use this handy class to avoid complex casting and pointer manipulation
  struct FunctionArguments {
    storage::StorageManager *storage_manager;
    executor::ExecutorContext *executor_context;
    QueryParameters *query_parameters;
    char *consumer_arg;
    char rest[0];
  } PACKED;

  // Set up the function arguments
  auto *func_args = reinterpret_cast<FunctionArguments *>(param);
  func_args->storage_manager = storage::StorageManager::GetInstance();
  func_args->executor_context = context;
  func_args->query_parameters = &context->GetParams();
  func_args->consumer_arg = consumer.GetConsumerState();

  // Timer
  auto timer = new Timer<std::ratio<1, 1000>>;
  timer->Start();

  // Call init
  LOG_TRACE("Calling query's init() ...");
  try {
    init_func_(param);
  } catch (...) {
    // Cleanup if an exception is encountered
    tear_down_func_(param);
    throw;
  }

  // Time initialization
  if (stats != nullptr) {
    timer->Stop();
    stats->init_ms = timer->GetDuration();
    timer->Reset();
    timer->Start();
  }

  auto on_execute_stages_complete = [this, param, context, timer, stats, on_complete] {
    // Timer plan execution
    if (stats != nullptr) {
      timer->Stop();
      stats->plan_ms = timer->GetDuration();
      timer->Reset();
      timer->Start();
    }

    // Clean up
    LOG_TRACE("Calling query's tearDown() ...");
    tear_down_func_(param);

    // No need to cleanup if we get an exception while cleaning up...
    if (stats != nullptr) {
      timer->Stop();
      stats->tear_down_ms = timer->GetDuration();
    }

    executor::ExecutionResult result;
    result.m_result = ResultType::SUCCESS;
    result.m_processed = context->num_processed;
    on_complete(result);

    delete[] param;
    delete context;
    delete timer;
  };

  ExecuteStages(stages_.begin(), stages_.end(), param, on_execute_stages_complete);
}

bool Query::Prepare(const QueryFunctions &query_funcs) {
  LOG_TRACE("Going to JIT the query ...");

  // Compile the code
  if (!code_context_.Compile()) {
    return false;
  }

  LOG_TRACE("Setting up Query ...");

  // Get pointers to the JITed functions
  init_func_ = (compiled_function_t)code_context_.GetRawFunctionPointer(
      query_funcs.init_func);
  PL_ASSERT(init_func_ != nullptr);

  for (auto &stage : query_funcs.stages) {
    auto func_ptr = (compiled_function_t)code_context_.GetRawFunctionPointer(
        stage.llvm_func_);
    PL_ASSERT(func_ptr != nullptr);
    stages_.push_back(Stage {
        .kind_ = stage.kind_,
        .func_ptr_ = func_ptr,
    });
  }

  tear_down_func_ = (compiled_function_t)code_context_.GetRawFunctionPointer(
      query_funcs.tear_down_func);
  PL_ASSERT(tear_down_func_ != nullptr);

  LOG_TRACE("Query has been setup ...");

  // All is well
  return true;
}

}  // namespace codegen
}  // namespace peloton
