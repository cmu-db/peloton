//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_statement.cpp
//
// Identification: src/codegen/query_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query.h"

#include "catalog/catalog.h"
#include "executor/executor_context.h"
#include "common/macros.h"

namespace peloton {
namespace codegen {

// Constructor
Query::Query(const planner::AbstractPlan &query_plan)
    : query_plan_(query_plan) {}

// Execute the query on the given database (and within the provided transaction)
// This really involves calling the init(), plan() and tearDown() functions, in
// that order. We also need to correctly handle cases where _any_ of those
// functions throw exceptions.
void Query::Execute(concurrency::Transaction &txn, char *consumer_arg,
                    RuntimeStats *stats, executor::ExecutorContext *exec_context) {
  CodeGen codegen{GetCodeContext()};

  llvm::Type *runtime_state_type = runtime_state_.FinalizeType(codegen);
  uint64_t parameter_size = codegen.SizeOf(runtime_state_type);
  PL_ASSERT(parameter_size % 8 == 0);

  // Finalize parameters (for parameterization)
  PrepareParams(exec_context);

  // Allocate some space for the function arguments
  std::unique_ptr<char[]> param_data{new char[parameter_size]};
  // Allocate space for storing runtime params (for parameterization)
  std::unique_ptr<char *[]> char_ptr_params{new char*[params_.size()]};
  std::unique_ptr<int32_t[]> char_len_params{new int32_t[params_.size()]};

  // Grab an non-owning pointer to the space
  char *param = param_data.get();

  // Clean the space
  PL_MEMSET(param, 0, parameter_size);

  // We use this handy class to avoid complex casting and pointer manipulation
  struct FunctionArguments {
    concurrency::Transaction *txn;
    catalog::Catalog *catalog;
    char **char_ptr_params;
    int32_t *char_len_params;
    Target *update_target_list;
    DirectMap *update_direct_list;
    executor::ExecutorContext *exec_context;
    char *consumer_arg;
    char rest[0];
  } PACKED;

  // Set up the function arguments
  auto *func_args = reinterpret_cast<FunctionArguments *>(param_data.get());
  func_args->txn = &txn;
  func_args->catalog = catalog::Catalog::GetInstance();
  func_args->char_ptr_params = char_ptr_params.get();
  func_args->char_len_params = char_len_params.get();
  func_args->update_target_list = update_target_list_.data();
  func_args->update_direct_list = update_direct_list_.data();
  func_args->exec_context = exec_context;
  func_args->consumer_arg = consumer_arg;

  // dynamic storage for serializing parameters (for parameterization)
  std::vector<std::unique_ptr<char []>> params;
  // load parameters into runtime state
  LoadParams(params, func_args->char_ptr_params, func_args->char_len_params);

  // Timer
  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  // Call init
  LOG_DEBUG("Calling query's init() ...");
  try {
    init_func_(param);
  } catch (...) {
    // Cleanup if an exception is encountered
    tear_down_func_(param);
    throw;
  }

  // Time initialization
  if (stats != nullptr) {
    timer.Stop();
    stats->init_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Execute the query!
  LOG_DEBUG("Calling query's plan() ...");
  try {
    plan_func_(param);
  } catch (...) {
    // Cleanup if an exception is encountered
    tear_down_func_(param);
    throw;
  }

  // Timer plan execution
  if (stats != nullptr) {
    timer.Stop();
    stats->plan_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Clean up
  LOG_DEBUG("Calling query's tearDown() ...");
  tear_down_func_(param);

  // No need to cleanup if we get an exception while cleaning up...
  if (stats != nullptr) {
    timer.Stop();
    stats->tear_down_ms = timer.GetDuration();
  }
}

bool Query::Prepare(const QueryFunctions &query_funcs) {
  LOG_TRACE("Going to JIT the query ...");

  // Compile the code
  if (!code_context_.Compile()) {
    return false;
  }

  LOG_TRACE("Setting up Query ...");

  // Get pointers to the JITed functions
  init_func_ = (compiled_function_t)code_context_.GetFunctionPointer(
      query_funcs.init_func);
  PL_ASSERT(init_func_ != nullptr);

  plan_func_ = (compiled_function_t)code_context_.GetFunctionPointer(
      query_funcs.plan_func);
  PL_ASSERT(plan_func_ != nullptr);

  tear_down_func_ = (compiled_function_t)code_context_.GetFunctionPointer(
      query_funcs.tear_down_func);
  PL_ASSERT(tear_down_func_ != nullptr);

  LOG_TRACE("Query has been setup ...");

  // All is well
  return true;
}

uint32_t Query::StoreParam(Parameter param, int idx) {
  if (idx < 0) {
    params_.emplace_back(param);
    return params_.size() - 1;
  } else {
    PL_ASSERT((uint32_t)idx < params_.size());
    params_[idx] = param;
    return 0;
  }
}

void Query::PrepareParams(executor::ExecutorContext *exec_context) {
  for (uint32_t i = 0; i < params_.size(); i ++) {
    type::Value value = params_[i].GetValue();
    switch (params_[i].GetType()) {
      case Parameter::ParamType::Const:
        break;
      case Parameter::ParamType::Param:
        PL_ASSERT(exec_context != nullptr);
        value = exec_context->GetParams().at(
                params_[i].GetParamIdx()).CastAs(params_[i].GetValueType());
        StoreParam(Parameter::GetConstValParamInstance(value), i);
        break;
      default: {
        throw Exception{"Unknown Param Type"};
      }
    }
  }
}

void Query::LoadParams(std::vector<std::unique_ptr<char[]>> &params,
                       char **char_ptr_params,
                       int32_t *char_len_params) {
  for (uint32_t i = 0; i < params_.size(); i ++) {
    type::Value value = params_[i].GetValue();
    switch (value.GetTypeId()) {
      case type::Type::TypeId::TINYINT: {
        params.emplace_back(std::unique_ptr<char[]>{new char[sizeof(int8_t)]});
        *reinterpret_cast<int8_t *>(params[i].get()) = type::ValuePeeker::PeekTinyInt(value);
        char_ptr_params[i] = params[i].get();
        break;
      }
      case type::Type::TypeId::SMALLINT: {
        params.emplace_back(std::unique_ptr<char[]>{new char[sizeof(int16_t)]});
        *reinterpret_cast<int16_t *>(params[i].get()) = type::ValuePeeker::PeekSmallInt(value);
        char_ptr_params[i] = params[i].get();
        break;
      }
      case type::Type::TypeId::INTEGER: {
        params.emplace_back(std::unique_ptr<char[]>{new char[sizeof(int32_t)]});
        *reinterpret_cast<int32_t *>(params[i].get()) = type::ValuePeeker::PeekInteger(value);
        char_ptr_params[i] = params[i].get();
        break;
      }
      case type::Type::TypeId::BIGINT: {
        params.emplace_back(std::unique_ptr<char[]>{new char[sizeof(int64_t)]});
        *reinterpret_cast<int64_t *>(params[i].get()) = type::ValuePeeker::PeekBigInt(value);
        char_ptr_params[i] = params[i].get();
        break;
      }
      case type::Type::TypeId::DECIMAL: {
        params.emplace_back(std::unique_ptr<char[]>{new char[sizeof(double)]});
        *reinterpret_cast<double *>(params[i].get()) = type::ValuePeeker::PeekDouble(value);
        char_ptr_params[i] = params[i].get();
        break;
      }
      case type::Type::TypeId::DATE: {
        params.emplace_back(std::unique_ptr<char[]>{new char[sizeof(int32_t)]});
        *reinterpret_cast<int32_t *>(params[i].get()) = type::ValuePeeker::PeekDate(value);
        char_ptr_params[i] = params[i].get();
        break;
      }
      case type::Type::TypeId::TIMESTAMP: {
        params.emplace_back(std::unique_ptr<char[]>{new char[sizeof(uint64_t)]});
        *reinterpret_cast<uint64_t *>(params[i].get()) = type::ValuePeeker::PeekTimestamp(value);
        char_ptr_params[i] = params[i].get();
        break;
      }
      case type::Type::TypeId::VARCHAR: {
        const char *str = type::ValuePeeker::PeekVarchar(value);
        params.emplace_back(std::unique_ptr<char[]>{new char[value.GetLength()]});
        PL_MEMCPY(params[i].get(), str, value.GetLength());
        char_ptr_params[i] = params[i].get();
        char_len_params[i] = value.GetLength();
        break;
      }
      default: {
        throw Exception{"Unknown param value type " +
                        TypeIdToString(value.GetTypeId())};
      }
    }
  }
}

}  // namespace codegen
}  // namespace peloton