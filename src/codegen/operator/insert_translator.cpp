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

#include "codegen/proxy/inserter_proxy.h"
#include "codegen/proxy/query_parameters_proxy.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/proxy/tuple_proxy.h"
#include "codegen/operator/insert_translator.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

InsertTranslator::InsertTranslator(const planner::InsertPlan &insert_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(insert_plan, context, pipeline),
      table_storage_(*insert_plan.GetTable()->GetSchema()) {
  // Create the translator for its child only when there is a child
  if (insert_plan.GetChildrenSize() != 0) {
    context.Prepare(*insert_plan.GetChild(0), pipeline);
  }

  // Register the inserter instance as state
  inserter_state_id_ = context.GetQueryState().RegisterState(
      "inserter", InserterProxy::GetType(GetCodeGen()));
}

const planner::InsertPlan &InsertTranslator::GetInsertPlan() const {
  return GetPlanAs<planner::InsertPlan>();
}

void InsertTranslator::InitializeQueryState() {
  CodeGen &codegen = GetCodeGen();

  storage::DataTable *table = GetInsertPlan().GetTable();
  llvm::Value *table_ptr = codegen.Call(
      StorageManagerProxy::GetTableWithOid,
      {GetStorageManagerPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});

  // Initialize the inserter with txn and table
  llvm::Value *inserter = LoadStatePtr(inserter_state_id_);
  codegen.Call(InserterProxy::Init,
               {inserter, table_ptr, GetExecutorContextPtr()});
}

void InsertTranslator::Produce() const {
  /// If the insert plan has a child, it means we have an insert-from-select. In
  /// this case, we let the child produce the tuples we'll insert in Consume().
  /// If the insert plan doesn't have a child operator, we're directly inserting
  /// an explicit list of tuples stored in the plan itself.

  if (GetInsertPlan().GetChildrenSize() != 0) {
    /// First case
    GetCompilationContext().Produce(*GetInsertPlan().GetChild(0));
  } else {
    /// Regular insert with constants
    auto producer = [this](UNUSED_ATTRIBUTE ConsumerContext &ctx) {
      CodeGen &codegen = GetCodeGen();
      auto *inserter = LoadStatePtr(inserter_state_id_);

      const auto &insert_plan = GetInsertPlan();
      auto num_tuples = insert_plan.GetBulkInsertCount();
      auto num_columns = insert_plan.GetTable()->GetSchema()->GetColumnCount();

      // Read tuple data from the parameter storage and insert
      const auto &parameter_cache = GetCompilationContext().GetParameterCache();
      for (uint32_t tuple_idx = 0; tuple_idx < num_tuples; tuple_idx++) {
        auto *tuple_ptr =
            codegen.Call(InserterProxy::AllocateTupleStorage, {inserter});
        auto *pool = codegen.Call(InserterProxy::GetPool, {inserter});

        // Transform into the codegen values and store values in the tuple
        // storage
        std::vector<codegen::Value> values;
        for (uint32_t column_id = 0; column_id < num_columns; column_id++) {
          auto value =
              parameter_cache.GetValue(column_id + tuple_idx * num_columns);
          values.push_back(value);
        }
        table_storage_.StoreValues(codegen, tuple_ptr, values, pool);

        // Complete the insertion
        codegen.Call(InserterProxy::Insert, {inserter});
      }
    };

    /// Execute insertion in separate pipeline serially
    GetPipeline().RunSerial(producer);
  }
}

void InsertTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  CodeGen &codegen = GetCodeGen();

  // Ask the Inserter to allocate space for the row we received
  auto *inserter = LoadStatePtr(inserter_state_id_);
  auto *tuple_ptr =
      codegen.Call(InserterProxy::AllocateTupleStorage, {inserter});
  auto *pool = codegen.Call(InserterProxy::GetPool, {inserter});

  // Materialize tuple data from row and attribute information
  std::vector<codegen::Value> values;
  for (const auto *ai : GetInsertPlan().GetAttributeInfos()) {
    codegen::Value v = row.DeriveValue(codegen, ai);
    values.push_back(v);
  }
  table_storage_.StoreValues(codegen, tuple_ptr, values, pool);

  // Call Inserter to insert the reserved tuple storage area
  codegen.Call(InserterProxy::Insert, {inserter});
}

void InsertTranslator::TearDownQueryState() {
  // Tear down the inserter
  llvm::Value *inserter = LoadStatePtr(inserter_state_id_);
  GetCodeGen().Call(InserterProxy::TearDown, {inserter});
}

}  // namespace codegen
}  // namespace peloton
