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


namespace peloton {
namespace executor {


//Constructor for drop executor
DropExecutor::DropExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {global_catalog = nullptr;}

// Initialize executer
bool DropExecutor::DInit() {
	LOG_INFO("Initializing Drop Executer...");
  auto &bootstrapper = catalog::Bootstrapper::GetInstance();
  LOG_INFO("Bootstrapping catalog...");
  global_catalog = bootstrapper.bootstrap();
  LOG_INFO("Creating default database...");
  global_catalog->CreateDatabase("default_database");
  return true;
}

bool DropExecutor::DExecute() {
  const planner::DropPlan &node = GetPlanNode<planner::DropPlan>();
  std::string table_name = node.GetTableName();
  global_catalog->DropTable("default_database", table_name);
  return true;
}

}
}
