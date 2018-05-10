//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// explain_executor.cpp
//
// Identification: src/executor/explain_executor.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/logger.h"
#include "executor/explain_executor.h"
#include "executor/executor_context.h"
#include "executor/logical_tile_factory.h"
#include "optimizer/optimizer.h"
#include "storage/tile.h"
#include "type/type.h"

namespace peloton {
namespace executor {

bool ExplainExecutor::DInit() {
  LOG_TRACE("Initializing explain executor...");
  LOG_TRACE("Explain executor initialized!");
  return true;
}

bool ExplainExecutor::DExecute() {
  LOG_TRACE("Executing Explain...");

  const planner::ExplainPlan &node = GetPlanNode<planner::ExplainPlan>();

  parser::SQLStatement *sql_stmt = node.GetSQLStatement();

  LOG_TRACE("EXPLAIN : %s", sql_stmt->GetInfo().c_str());

  auto current_txn = executor_context_->GetTransaction();

  auto bind_node_visitor =
      binder::BindNodeVisitor(current_txn, node.GetDatabaseName());

  // Bind, optimize and return the plan as a string
  bind_node_visitor.BindNameToNode(sql_stmt);
  std::unique_ptr<optimizer::Optimizer> optimizer(new optimizer::Optimizer());
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      new parser::SQLStatementList(sql_stmt));
  auto plan = optimizer->BuildPelotonPlanTree(stmt_list, current_txn);
  const catalog::Schema schema({catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "Query Plan")});
  std::shared_ptr<storage::Tile> dest_tile(
      storage::TileFactory::GetTempTile(schema, 1));
  std::unique_ptr<storage::Tuple> buffer(new storage::Tuple(&schema, true));
  buffer->SetValue(0, type::ValueFactory::GetVarcharValue(plan->GetInfo()));
  dest_tile->InsertTuple(0, buffer.get());
  SetOutput(LogicalTileFactory::WrapTiles({dest_tile}));

  LOG_TRACE("Explain finished!, plan : %s", plan->GetInfo().c_str());
  return false;
}

}  // namespace executor
}  // namespace peloton
