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
#include "storage/tile_group.h"

namespace peloton {
namespace codegen {

// Constructor
Query::Query(const planner::AbstractPlan &query_plan)
    : query_plan_(query_plan) {}

static void ExecuteStages(concurrency::TransactionContext *txn,
                          std::vector<Stage>::iterator begin,
                          std::vector<Stage>::iterator end, char *param,
                          std::function<void()> on_complete) {
  if (begin == end) {
    on_complete();
  } else {
    auto &pool = threadpool::MonoQueuePool::GetInstance();
    switch (begin->kind_) {
      case StageKind::SINGLETHREADED: {
        pool.SubmitTask([&pool, txn, begin, end, param, on_complete] {
          begin->singlethreaded_func_(param);
          ExecuteStages(txn, begin + 1, end, param, on_complete);
        });
        break;
      }
      case StageKind::MULTITHREADED_SEQSCAN: {
        auto table = begin->table_;
        auto ntile_groups = table->GetTileGroupCount();

        for (size_t i = 0; i < ntile_groups; ++i) {
          auto tilegroup = table->GetTileGroup(i);
          txn->RecordTouchTileGroup(tilegroup->GetTileGroupId());
        }

        size_t ntasks = std::min(pool.NumWorkers(), ntile_groups);
        auto ntile_groups_per_task = ntile_groups / ntasks;
        auto count = new std::atomic_size_t(ntasks);

        begin->multithreaded_seqscan_init_(param, ntasks);

        if (ntasks == 0) {
          begin->multithreaded_seqscan_func_(param, 0, 0, ntile_groups);
          ExecuteStages(txn, begin + 1, end, param, on_complete);
        } else {
          for (size_t task = 0; task < ntasks; ++task) {
            size_t tile_group_beg = ntile_groups_per_task * task;
            size_t tile_group_end = (task + 1 == ntasks)
                                        ? ntile_groups
                                        : ntile_groups_per_task * (task + 1);
            LOG_DEBUG("Scanning [%zu, %zu)", tile_group_beg, tile_group_end);
            pool.SubmitTask(
                [&pool, txn, task, begin, end, param, tile_group_beg,
                 tile_group_end, count, on_complete] {
                  begin->multithreaded_seqscan_func_(
                      param, task, tile_group_beg, tile_group_end);
                  if (--(*count) == 0) {
                    delete count;
                    ExecuteStages(txn, begin + 1, end, param, on_complete);
                  }
                });
          }
        }
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

  auto on_execute_stages_complete =
      [this, param, context, timer, stats, on_complete] {
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

  ExecuteStages(context->GetTransaction(), stages_.begin(), stages_.end(),
                param, on_execute_stages_complete);
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

  for (auto &codegen_stage : query_funcs.stages) {
    Stage stage;
    stage.kind_ = codegen_stage.kind_;
    switch (stage.kind_) {
      case StageKind::SINGLETHREADED: {
        auto func = code_context_.GetRawFunctionPointer(
            codegen_stage.singlethreaded_func_);
        stage.singlethreaded_func_ = (void (*)(char *))func;
        break;
      }
      case StageKind::MULTITHREADED_SEQSCAN: {
        auto init_func = code_context_.GetRawFunctionPointer(
            codegen_stage.multithreaded_seqscan_init_);

        auto scan_func = code_context_.GetRawFunctionPointer(
            codegen_stage.multithreaded_seqscan_func_);

        stage.multithreaded_seqscan_init_ = (void (*)(char *, size_t))init_func;

        stage.multithreaded_seqscan_func_ =
            (void (*)(char *, size_t, size_t, size_t))scan_func;

        stage.table_ = codegen_stage.table_;

        break;
      }
    }

    stages_.push_back(stage);
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
