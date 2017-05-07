//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_function_executor.cpp
//
// Identification: src/executor/create_function_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/create_function_executor.h"
#include "executor/executor_context.h"
#include "common/logger.h"
#include "catalog/catalog.h"
#include "catalog/function_catalog.h"

#include <vector>

namespace peloton {
namespace executor {

// Constructor for drop executor
CreateFunctionExecutor::CreateFunctionExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
  context = executor_context;
}

// Initialize executer
// Nothing to initialize now
bool CreateFunctionExecutor::DInit() {
  LOG_TRACE("Initializing Create Function Executer...");
  LOG_TRACE("Create Function Executer initialized!");
  return true;
}

bool CreateFunctionExecutor::DExecute() {
  LOG_TRACE("Executing Create...");
  UNUSED_ATTRIBUTE const planner::CreateFunctionPlan &node = GetPlanNode<planner::CreateFunctionPlan>();
  UNUSED_ATTRIBUTE auto current_txn = context->GetTransaction();
  auto pool = context->GetPool();
  
  auto proname = node.GetFunctionName();
  auto prolang = node.GetUDFLanguage();
  auto pronargs = node.GetNumParams();
  auto prorettype = node.GetReturnType();
  auto proargtypes = node.GetFunctionParameterTypes();
  auto proargnames = node.GetFunctionParameterNames();
  auto prosrc_bin = node.GetFunctionBody();
 
  std::vector<type::Type::TypeId> dummy; 
  std::vector<int> dummy2;
  std::vector<std::string> dummy3; 
  oid_t dummy4 = 0;
  float dummy5 = 0.0;
  char dummy6 = 'a';
  int dummy7 = 0;

  ResultType result = catalog::FunctionCatalog::GetInstance()->InsertFunction(proname,dummy4,dummy4,prolang,dummy5,dummy5,dummy4,false,false,false,false,false,false,dummy6,pronargs,dummy7,prorettype,proargtypes,dummy,dummy2,proargnames,dummy,prosrc_bin,dummy3,dummy2,pool,current_txn);

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
}
}
