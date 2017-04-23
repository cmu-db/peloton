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
    : query_plan_(query_plan) {
  int_8_type_cnt_ = 0;
  int_16_type_cnt_ = 0;
  int_32_type_cnt_ = 0;
  int_64_type_cnt_ = 0;
  double_type_cnt_ = 0;
  varchar_type_cnt_ = 0;
}

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

  // Allocate some space for the function arguments
  std::unique_ptr<char[]> param_data{new char[parameter_size]};
  std::unique_ptr<int8_t[]> int_8_params{new int8_t[int_8_type_cnt_]};
  std::unique_ptr<int16_t[]> int_16_params{new int16_t[int_16_type_cnt_]};
  std::unique_ptr<int32_t[]> int_32_params{new int32_t[int_32_type_cnt_]};
  std::unique_ptr<int64_t[]> int_64_params{new int64_t[int_64_type_cnt_]};
  std::unique_ptr<double[]> double_params{new double[double_type_cnt_]};
  std::unique_ptr<char *[]> char_ptr_params{new char*[varchar_type_cnt_]};
  std::unique_ptr<int32_t[]> char_len_params{new int32_t[varchar_type_cnt_]};

  // Grab an non-owning pointer to the space
  char *param = param_data.get();

  // Clean the space
  PL_MEMSET(param, 0, parameter_size);

  // We use this handy class to avoid complex casting and pointer manipulation
  struct FunctionArguments {
    concurrency::Transaction *txn;
    catalog::Catalog *catalog;
    int8_t *int_8_params;
    int16_t *int_16_params;
    int32_t *int_32_params;
    int64_t *int_64_params;
    double *double_params;
    char **char_ptr_params;
    int32_t *char_len_params;
    char *consumer_arg;
    char rest[0];
  } PACKED;

  // Set up the function arguments
  auto *func_args = reinterpret_cast<FunctionArguments *>(param_data.get());
  func_args->txn = &txn;
  func_args->catalog = catalog::Catalog::GetInstance();
  func_args->int_8_params = int_8_params.get();
  func_args->int_16_params = int_16_params.get();
  func_args->int_32_params = int_32_params.get();
  func_args->int_64_params = int_64_params.get();
  func_args->double_params = double_params.get();
  func_args->char_ptr_params = char_ptr_params.get();
  func_args->char_len_params = char_len_params.get();
  func_args->consumer_arg = consumer_arg;

  uint32_t int_8_cnt = 0;
  uint32_t int_16_cnt = 0;
  uint32_t int_32_cnt = 0;
  uint32_t int_64_cnt = 0;
  uint32_t double_cnt = 0;
  uint32_t varchar_cnt = 0;
  for (uint32_t i = 0; i < params_.size(); i ++) {
    type::Value value = params_[i].GetValue();
    switch (params_[i].GetType()) {
      case Parameter::ParamType::Const:
        break;
      case Parameter::ParamType::Param:
        PL_ASSERT(exec_context != nullptr);
        value = exec_context->GetParams().at(
                params_[i].GetParamIdx());
        break;
      default: {
        throw Exception{"Unknown Param Type"};
      }
    }
    switch (value.GetTypeId()) {
      case type::Type::TypeId::TINYINT: {
          int_8_params[int_8_cnt ++] = type::ValuePeeker::PeekTinyInt(value);
          break;
      }
      case type::Type::TypeId::SMALLINT: {
          int_16_params[int_16_cnt ++] = type::ValuePeeker::PeekSmallInt(value);
          break;
      }
      case type::Type::TypeId::INTEGER: {
          int_32_params[int_32_cnt ++] = type::ValuePeeker::PeekInteger(value);
          break;
      }
      case type::Type::TypeId::BIGINT: {
          int_64_params[int_64_cnt ++] = type::ValuePeeker::PeekBigInt(value);
          break;
      }
      case type::Type::TypeId::DECIMAL: {
          double_params[double_cnt ++] = type::ValuePeeker::PeekDouble(value);
          break;
      }
      case type::Type::TypeId::DATE: {
          int_32_params[int_32_cnt ++] = type::ValuePeeker::PeekDate(value);
          break;
      }
      case type::Type::TypeId::TIMESTAMP: {
          int_64_params[int_64_cnt ++] = type::ValuePeeker::PeekTimestamp(value);
          break;
      }
      case type::Type::TypeId::VARCHAR: {
          std::string str = type::ValuePeeker::PeekVarchar(value);
          char c_str[str.length() + 1];
          strcpy(c_str, str.c_str());
          char_ptr_params[varchar_cnt] = c_str;
          char_len_params[varchar_cnt ++] = str.length();
          break;
      }
      default: {
          throw Exception{"Unknown param value type " +
                          TypeIdToString(value.GetTypeId())};
      }
    }
  }

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

uint32_t Query::StoreParam(Parameter param) {
  params_.emplace_back(param);
  uint32_t offset = 0;
  switch (param.GetValue().GetTypeId()) {
    case type::Type::TINYINT: {
      offset = int_8_type_cnt_ ++;
      break;
    }
    case type::Type::SMALLINT: {
      offset = int_16_type_cnt_ ++;
      break;
    }
    case type::Type::INTEGER: {
      offset = int_32_type_cnt_ ++;
      break;
    }
    case type::Type::BIGINT: {
      offset = int_64_type_cnt_ ++;
      break;
    }
    case type::Type::DECIMAL: {
      offset = double_type_cnt_ ++;
      break;
    }
    case type::Type::DATE: {
      offset = int_32_type_cnt_ ++;
      break;
    }
    case type::Type::TIMESTAMP: {
      offset = int_64_type_cnt_ ++;
      break;
    }
    case type::Type::VARCHAR: {
      offset = varchar_type_cnt_ ++;
      break;
    }
    default: {
      throw Exception{"Unknown param value type " +
                      TypeIdToString(param.GetValue().GetTypeId())};
    }
  }
  return offset;
}

}  // namespace codegen
}  // namespace peloton