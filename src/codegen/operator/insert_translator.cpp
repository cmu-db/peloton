//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_translator.cpp
//
// Identification: src/codegen/operator/insert_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/catalog_proxy.h"
#include "codegen/proxy/inserter_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/proxy/tuple_proxy.h"
#include "codegen/proxy/query_parameters_proxy.h"
#include "codegen/operator/insert_translator.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

InsertTranslator::InsertTranslator(const planner::InsertPlan &insert_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), insert_plan_(insert_plan),
      table_storage_(*insert_plan.GetTable()->GetSchema()) {

  // Create the translator for its child only when there is a child
  if (insert_plan.GetChildrenSize() != 0) {
    context.Prepare(*insert_plan.GetChild(0), pipeline);
  }

  // Register the inserter's runtime state to approach it throughout this
  inserter_state_id_ = context.GetRuntimeState().RegisterState(
      "inserter", InserterProxy::GetType(GetCodeGen()));
}

void InsertTranslator::InitializeState() {
  auto &codegen = GetCodeGen();
  auto &context = GetCompilationContext();

  storage::DataTable *table = insert_plan_.GetTable();
  llvm::Value *table_ptr = codegen.Call(StorageManagerProxy::GetTableWithOid,
      {GetCatalogPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});
 
  llvm::Value *executor_ptr = context.GetExecutorContextPtr();

  // Initialize the inserter with txn and table
  llvm::Value *inserter = LoadStatePtr(inserter_state_id_);
  codegen.Call(InserterProxy::Init, {inserter, table_ptr, executor_ptr});
}

void InsertTranslator::Produce() const {
  auto &context = GetCompilationContext();
  if (insert_plan_.GetChildrenSize() != 0) {
    // Produce on its child(a scan), to produce the tuples to be inserted
    context.Produce(*insert_plan_.GetChild(0));
  }
  else {
    auto &codegen = GetCodeGen();
    auto *inserter = LoadStatePtr(inserter_state_id_);

    auto num_tuples = insert_plan_.GetBulkInsertCount();
    auto num_columns = insert_plan_.GetTable()->GetSchema()->GetColumnCount();

    // Read tuple data from the parameter storage and insert
    for (uint32_t tuple_idx = 0; tuple_idx < num_tuples; tuple_idx++) {
      auto *tuple_ptr = codegen.Call(InserterProxy::AllocateTupleStorage,
                                     {inserter});
      auto *pool = codegen.Call(InserterProxy::GetPool, {inserter});

      // Trasform into the codegen values and store values in the tuple storage
      std::vector<codegen::Value> values;
      for (uint32_t column_id = 0; column_id< num_columns; column_id++) {
        auto v = context.GetParameterStorage().GetValue(codegen,
            column_id + tuple_idx * num_columns);
        values.push_back(v);
      }
      table_storage_.StoreValues(codegen, tuple_ptr, values, pool);

      // Complete the insertion
      codegen.Call(InserterProxy::Insert, {inserter});
    }
  }
}

void InsertTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();
  auto *inserter = LoadStatePtr(inserter_state_id_);

  auto *tuple_ptr = codegen.Call(InserterProxy::AllocateTupleStorage,
                                 {inserter});
  auto *pool = codegen.Call(InserterProxy::GetPool, {inserter});

  // Generate/Materialize tuple data from row and attribute information
  std::vector<codegen::Value> values;
  auto &ais = insert_plan_.GetAttributeInfos();
  for (const auto *ai : ais) {
    codegen::Value v = row.DeriveValue(codegen, ai);
    values.push_back(v);
  }
  table_storage_.StoreValues(codegen, tuple_ptr, values, pool);

  // Call Inserter to insert the reserved tuple storage area
  codegen.Call(InserterProxy::Insert, {inserter});
}

}  // namespace codegen
}  // namespace peloton
