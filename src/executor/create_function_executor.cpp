//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_function_executor.cpp
//
// Identification: src/executor/create_function_executor.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/create_function_executor.h"

#include "catalog/catalog.h"
#include "catalog/language_catalog.h"
#include "codegen/code_context.h"
#include "common/logger.h"
#include "concurrency/transaction_context.h"
#include "executor/executor_context.h"
#include "planner/create_function_plan.h"
#include "udf/udf_handler.h"

namespace peloton {
namespace executor {

CreateFunctionExecutor::CreateFunctionExecutor(
    const planner::AbstractPlan *node, ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

bool CreateFunctionExecutor::DInit() {
  LOG_TRACE("Initializing CreateFunctionExecutor...");
  return true;
}

bool CreateFunctionExecutor::DExecute() {
  LOG_TRACE("Executing Create...");
  const auto &node = GetPlanNode<planner::CreateFunctionPlan>();
  auto *current_txn = executor_context_->GetTransaction();

  auto proname = node.GetFunctionName();
  oid_t prolang = catalog::LanguageCatalog::GetInstance()
                      .GetLanguageByName("plpgsql", current_txn)
                      ->GetOid();
  auto prorettype = node.GetReturnType();
  auto proargtypes = node.GetFunctionParameterTypes();
  auto proargnames = node.GetFunctionParameterNames();
  auto prosrc = node.GetFunctionBody()[0];

  // TODO(PP) : Check and handle Replace

  // Pass it off to the UDF handler. Once the UDF is compiled, put that along
  // with the other UDF details into the catalog
  udf::UDFHandler udf_handler;

  auto code_context = udf_handler.Execute(current_txn, proname, prosrc,
                                          proargnames, proargtypes, prorettype);

  // The result
  ResultType result;

  // Get LLVM Function Pointer for the compiled UDF
  auto func_ptr = code_context->GetUDF();
  if (func_ptr != nullptr) {
    // Insert into catalog
    catalog::Catalog::GetInstance()->AddPlpgsqlFunction(
        proname, proargtypes, prorettype, prolang, prosrc, code_context,
        current_txn);
    result = ResultType::SUCCESS;
  } else {
    result = ResultType::FAILURE;
  }

  current_txn->SetResult(result);

  if (current_txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Registered UDF successfully!");
  } else if (current_txn->GetResult() == ResultType::FAILURE) {
    LOG_TRACE("Could not register function. SAD.");
  } else {
    LOG_TRACE("Result is: %s",
              ResultTypeToString(current_txn->GetResult()).c_str());
  }

  return false;
}

}  // namespace executor
}  // namespace peloton
