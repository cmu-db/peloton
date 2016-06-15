//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_executor.cpp
//
// Identification: src/executor/create_executer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/create_executer.h"


namespace peloton {
namespace executor {


//Constructor for drop executor
CreateExecutor::CreateExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {global_catalog = nullptr;}

// Initialize executer
bool CreateExecutor::DInit() {
  auto &bootstrapper = catalog::Bootstrapper::GetInstance();
  global_catalog = bootstrapper.bootstrap();
  global_catalog->CreateDatabase("default_database");
  return true;
}

bool CreateExecutor::DExecute() {
  const planner::CreatePlan &node = GetPlanNode<planner::CreatePlan>();
  std::string table_name = node.GetTableName();
  std::unique_ptr<catalog::Schema> schema(node.GetSchema());
  global_catalog->CreateTable("default_database", table_name, std::move(schema));
  return true;
}

}
}
