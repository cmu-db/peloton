//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_executor.cpp
//
// Identification: src/executor/drop_executer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/drop_executer.h"

#include "catalog/bootstrapper.h"


namespace peloton {
namespace executor {


//Constructor for drop executor
DropExecutor::DropExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
     context = executor_context;
}

// Initialize executer
bool DropExecutor::DInit() {
  LOG_INFO("Initializing Drop Executer...");
  catalog::Bootstrapper::bootstrap();
  catalog::Bootstrapper::global_catalog->CreateDatabase("default_database");
  LOG_INFO("Drop Executer initialized!");
  return true;
}

bool DropExecutor::DExecute() {
  LOG_INFO("Executing Drop...");
  const planner::DropPlan &node = GetPlanNode<planner::DropPlan>();
  std::string table_name = node.GetTableName();
  Result result = catalog::Bootstrapper::global_catalog->DropTable("default_database", table_name);
  context->GetTransaction()->SetResult(result);
  if(context->GetTransaction()->GetResult() == Result::RESULT_SUCCESS){
	  LOG_INFO("Dropping table succeeded!");
  }
  else if(context->GetTransaction()->GetResult() == Result::RESULT_FAILURE && node.IsMissing()) {
	  context->GetTransaction()->SetResult(Result::RESULT_SUCCESS);
	  LOG_INFO("Dropping table Succeeded!");
  }
  else if(context->GetTransaction()->GetResult() == Result::RESULT_FAILURE && !node.IsMissing()){
	  LOG_INFO("Dropping table Failed!");
  }
  else {
	  LOG_INFO("Result is: %d", context->GetTransaction()->GetResult());
  }
  return false;
}

}
}
