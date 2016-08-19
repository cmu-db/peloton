//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_executor.cpp
//
// Identification: src/executor/create_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "../include/executor/create_executor.h"

#include <vector>

#include "catalog/bootstrapper.h"

namespace peloton {
namespace executor {

//Constructor for drop executor
CreateExecutor::CreateExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
  context = executor_context;
}

// Initialize executer
// Nothing to initialize now
bool CreateExecutor::DInit() {
  LOG_TRACE("Initializing Create Executer...");
  LOG_TRACE("Create Executer initialized!");
  return true;

}

bool CreateExecutor::DExecute() {
  LOG_TRACE("Executing Create...");
  const planner::CreatePlan &node = GetPlanNode<planner::CreatePlan>();
  auto current_txn = context->GetTransaction();
  
  // Check if query was for creating table
  if(node.GetCreateType() == CreateType::CREATE_TYPE_TABLE){
    std::string table_name = node.GetTableName();
    std::unique_ptr<catalog::Schema> schema(node.GetSchema());


    Result result = catalog::Bootstrapper::global_catalog->CreateTable(DEFAULT_DB_NAME, table_name, std::move(schema), current_txn);
    current_txn->SetResult(result);

    if(current_txn->GetResult() == Result::RESULT_SUCCESS){
      LOG_TRACE("Creating table succeeded!");
    }
    else if(current_txn->GetResult() == Result::RESULT_FAILURE) {
      LOG_TRACE("Creating table failed!");
    }
    else {
      LOG_TRACE("Result is: %d", current_txn->GetResult());
    }
  }
  
  // Check if query was for creating index
  if(node.GetCreateType() == CreateType::CREATE_TYPE_INDEX){
    std::string table_name = node.GetTableName();
    std::string index_name = node.GetIndexName();
    bool unique_flag = node.IsUnique();
    
    auto index_attrs = node.GetIndexAttributes();

    Result result = catalog::Bootstrapper::global_catalog->CreateIndex(DEFAULT_DB_NAME, table_name, index_attrs , index_name , unique_flag);
    current_txn->SetResult(result);

    if(current_txn->GetResult() == Result::RESULT_SUCCESS){
      LOG_TRACE("Creating table succeeded!");
    }
    else if(current_txn->GetResult() == Result::RESULT_FAILURE) {
      LOG_TRACE("Creating table failed!");
    }
    else {
      LOG_TRACE("Result is: %d", current_txn->GetResult());
    }

  }
  return false;
}

}
}
