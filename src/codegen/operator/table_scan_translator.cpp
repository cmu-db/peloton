//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator.cpp
//
// Identification: src/codegen/operator/table_scan_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/table_scan_translator.h"

#include "codegen/function_builder.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/catalog_proxy.h"
#include "codegen/proxy/count_down_proxy.h"
#include "codegen/proxy/executor_thread_pool_proxy.h"
#include "codegen/proxy/task_info_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "planner/seq_scan_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// TABLE SCAN TRANSLATOR
//===----------------------------------------------------------------------===//

// Constructor
TableScanTranslator::TableScanTranslator(const planner::SeqScanPlan &scan,
                                         CompilationContext &context,
                                         Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      scan_(scan),
      table_(*scan_.GetTable()) {
  LOG_TRACE("Constructing TableScanTranslator ...");

  // The restriction, if one exists
  const auto *predicate = GetScanPlan().GetPredicate();

  if (predicate != nullptr) {
    // If there is a predicate, prepare a translator for it
    context.Prepare(*predicate);

    // If the scan's predicate is SIMDable, install a boundary at the output
    if (predicate->IsSIMDable()) {
      pipeline.InstallBoundaryAtOutput(this);
    }
  }

  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  selection_vector_id_ = runtime_state.RegisterState(
      "scanSelVec",
      codegen.ArrayType(codegen.Int32Type(), Vector::kDefaultVectorSize), true);

  auto task_info_type = TaskInfoProxy::GetType(codegen);
  task_info_vector_id_ = runtime_state.RegisterState(
      "scanTaskInfoVec",
      codegen.ArrayType(task_info_type, Vector::kDefaultVectorSize), true);

  count_down_id_ = runtime_state.RegisterState(
            "scanCountDown", CountDownProxy::GetType(codegen), false);

  LOG_DEBUG("Finished constructing TableScanTranslator ...");
}

void TableScanTranslator::TaskProduce(llvm::Value *tile_group_begin,
                                      llvm::Value *tile_group_end) const {
  auto &codegen = GetCodeGen();
  auto &table = GetTable();

  // Get the table instance from the database
  llvm::Value* catalog_ptr = GetCatalogPtr();
  llvm::Value* db_oid = codegen.Const32(table.GetDatabaseOid());
  llvm::Value* table_oid = codegen.Const32(table.GetOid());
  llvm::Value* table_ptr = codegen.Call(StorageManagerProxy::GetTableWithOid,
                                        {catalog_ptr, db_oid, table_oid});

  // The selection vector for the scan
  Vector sel_vec{LoadStateValue(selection_vector_id_),
                 Vector::kDefaultVectorSize, codegen.Int32Type()};

  // Generate the scan
  ScanConsumer scan_consumer{*this, sel_vec};
  table_.GenerateScan(codegen, table_ptr, tile_group_begin, tile_group_end,
                      sel_vec.GetCapacity(), scan_consumer);
}

// Produce!
void TableScanTranslator::Produce() const {
  auto &codegen = GetCodeGen();
  auto &compilation_context = GetCompilationContext();
  auto &runtime_state = compilation_context.GetRuntimeState();

  llvm::Value *catalog_ptr = GetCatalogPtr();
  llvm::Value *db_oid = codegen.Const32(GetTable().GetDatabaseOid());
  llvm::Value *table_oid = codegen.Const32(GetTable().GetOid());
  llvm::Value *table_ptr = codegen.Call(StorageManagerProxy::GetTableWithOid,
                                        {catalog_ptr, db_oid, table_oid});
  llvm::Value *ntile_groups = table_.GetTileGroupCount(codegen, table_ptr);
  codegen.CallPrintf("Number of tilegroups = %d\n", {ntile_groups});

  LOG_DEBUG("TableScan on [%u] starting to produce tuples ...",
            GetTable().GetOid());

  if (!compilation_context.MultithreadOn()) {
    TaskProduce(codegen.Const64(0), ntile_groups);

  } else {
    // Multithread On!
    FunctionBuilder task(
        codegen.GetCodeContext(),
        "task",
        codegen.VoidType(),
        {
            {"runtime_state", codegen.GetState()->getType()},
            {"task_info",     TaskInfoProxy::GetType(codegen)->getPointerTo()}
        }
    );
    {
      auto task_info_ptr = task.GetArgumentByPosition(1);
      auto task_id = codegen.Call(TaskInfoProxy::GetTaskId, {task_info_ptr});
      auto tile_group_begin =
          codegen.Call(TaskInfoProxy::GetBegin, {task_info_ptr});
      auto tile_group_end =
          codegen.Call(TaskInfoProxy::GetEnd, {task_info_ptr});
      codegen.CallPrintf("I am task %d [%zu, %zu)!\n",
                         {task_id, tile_group_begin, tile_group_end});

      TaskProduce(tile_group_begin, tile_group_end);

      auto count_down_ptr = runtime_state.LoadStatePtr(codegen, count_down_id_);
      codegen.Call(CountDownProxy::Decrease, {count_down_ptr});

      task.ReturnAndFinish();
    }
    auto task_func = task.GetFunction();

    // Initialize the CountDown.
    auto count_down_ptr = runtime_state.LoadStatePtr(codegen, count_down_id_);
    codegen.Call(CountDownProxy::Init, {count_down_ptr, codegen.Const32(1)});

    Vector task_info_vec{LoadStateValue(task_info_vector_id_),
                         Vector::kDefaultVectorSize,
                         TaskInfoProxy::GetType(codegen)};

    // TODO(zhixunt): This is a tunable parameter.
    llvm::Value *ntile_groups_per_task = ntile_groups;
    codegen.CallPrintf("Number of tilegroups per task = %zu\n",
                       {ntile_groups_per_task});

    llvm::Value *ntasks = codegen->CreateIntCast(codegen->CreateUDiv(
        ntile_groups,
        ntile_groups_per_task
    ), codegen.Int32Type(), /*isSigned=*/true);
    codegen.CallPrintf("Number of tasks = %zu\n", {ntasks});

    lang::Loop loop(
        codegen,
        codegen->CreateICmpSLT(codegen.Const32(0), ntasks),
        {
            {"task_id", codegen.Const32(0)},
            {"begin", codegen.Const64(0)},
            {"end", ntile_groups_per_task}
        }
    );
    {
      llvm::Value *curr_i = loop.GetLoopVar(0);
      llvm::Value *next_i = codegen->CreateAdd(curr_i, codegen.Const32(1));

      llvm::Value *curr_begin = loop.GetLoopVar(1);
      llvm::Value *next_begin = codegen->CreateAdd(curr_begin,
                                                   ntile_groups_per_task);

      llvm::Value *curr_end = loop.GetLoopVar(2);
      llvm::Value *next_end = codegen->CreateAdd(curr_end,
                                                 ntile_groups_per_task);

      llvm::Value *real_end;
      lang::If cond(codegen, codegen->CreateAnd(
          codegen->CreateICmpEQ(next_i, ntasks),
          codegen->CreateICmpULT(curr_end, ntile_groups)
      ));
      {
        real_end = ntile_groups;
      }
      cond.EndIf();
      real_end = cond.BuildPHI(real_end, curr_end);

      codegen.CallPrintf("Creating task %d, [%d, %d)\n",
                         {curr_i, curr_begin, real_end});

      auto task_info_ptr = task_info_vec.GetPtrToValue(codegen, curr_i);

      codegen.Call(TaskInfoProxy::Init, {
          task_info_ptr,
          /*task_id=*/curr_i,
          /*ntasks=*/ntasks,
          /*begin=*/curr_begin,
          /*end=*/real_end
      });

      // auto thread_pool = codegen::ExecutorThreadPool::GetInstance();
      auto thread_pool_ptr = codegen.Call(
          ExecutorThreadPoolProxy::GetInstance, {}
      );

      // using task_type = void (*)(char *ptr, TaskInfo *);
      auto task_info_type = TaskInfoProxy::GetType(codegen);
      auto task_func_type = llvm::FunctionType::get(
          codegen.VoidType(), {
              codegen.CharPtrType(),
              task_info_type->getPointerTo()
          },
          false
      )->getPointerTo();

      // thread_pool->SubmitTask(
      //   (char *)runtime_state,
      //   task_info,
      //   (task_type)task
      // );
      codegen.Call(codegen::ExecutorThreadPoolProxy::SubmitTask, {
          thread_pool_ptr,
          codegen->CreatePointerCast(codegen.GetState(), codegen.CharPtrType()),
          task_info_ptr,
          codegen->CreatePointerCast(task_func, task_func_type)
      });

      loop.LoopEnd(codegen->CreateICmpSLT(next_i, ntasks), {
          next_i, next_begin, next_end
      });
    }

    codegen.Call(CountDownProxy::Wait, {count_down_ptr});

    codegen.Call(CountDownProxy::Destroy, {count_down_ptr});

    lang::Loop loop_destroy(
        codegen,
        codegen->CreateICmpSLT(codegen.Const32(0), ntasks),
        {{"task_id_destroy", codegen.Const32(0)}}
    );
    {
      llvm::Value *curr_i = loop_destroy.GetLoopVar(0);
      llvm::Value *next_i = codegen->CreateAdd(curr_i, codegen.Const32(1));

      auto task_info_ptr = task_info_vec.GetPtrToValue(codegen, curr_i);
      codegen.Call(TaskInfoProxy::Destroy, {task_info_ptr});

      loop_destroy.LoopEnd(codegen->CreateICmpSLT(next_i, ntasks), {next_i});
    }
  }

  LOG_DEBUG("TableScan on [%u] finished producing tuples ...",
            GetTable().GetOid());
}

// Get the stringified name of this scan
std::string TableScanTranslator::GetName() const {
  std::string name = "Scan('" + GetTable().GetName() + "'";
  auto *predicate = GetScanPlan().GetPredicate();
  if (predicate != nullptr && predicate->IsSIMDable()) {
    name.append(", ").append(std::to_string(Vector::kDefaultVectorSize));
  }
  name.append(")");
  return name;
}

// Table accessor
const storage::DataTable &TableScanTranslator::GetTable() const {
  return *scan_.GetTable();
}

//===----------------------------------------------------------------------===//
// VECTORIZED SCAN CONSUMER
//===----------------------------------------------------------------------===//

// Constructor
TableScanTranslator::ScanConsumer::ScanConsumer(
    const TableScanTranslator &translator, Vector &selection_vector)
    : translator_(translator), selection_vector_(selection_vector) {}

// Generate the body of the vectorized scan
void TableScanTranslator::ScanConsumer::ProcessTuples(
    CodeGen &codegen, llvm::Value *tid_start, llvm::Value *tid_end,
    TileGroup::TileGroupAccess &tile_group_access) {
  // TODO: Should visibility check be done here or in codegen::Table/TileGroup?

  // 1. Filter the rows in the range [tid_start, tid_end) by txn visibility
  FilterRowsByVisibility(codegen, tid_start, tid_end, selection_vector_);

  // 2. Filter rows by the given predicate (if one exists)
  auto *predicate = GetPredicate();
  if (predicate != nullptr) {
    // First perform a vectorized filter, putting TIDs into the selection vector
    FilterRowsByPredicate(codegen, tile_group_access, tid_start, tid_end,
                          selection_vector_);
  }

  // 3. Setup the (filtered) row batch and setup attribute accessors
  RowBatch batch{translator_.GetCompilationContext(), tile_group_id_, tid_start,
                 tid_end, selection_vector_, true};

  std::vector<TableScanTranslator::AttributeAccess> attribute_accesses;
  SetupRowBatch(batch, tile_group_access, attribute_accesses);

  // 4. Push the batch into the pipeline
  ConsumerContext context{translator_.GetCompilationContext(),
                          translator_.GetPipeline()};
  context.Consume(batch);
}

void TableScanTranslator::ScanConsumer::SetupRowBatch(
    RowBatch &batch, TileGroup::TileGroupAccess &tile_group_access,
    std::vector<TableScanTranslator::AttributeAccess> &access) const {
  // Grab a hold of the stuff we need (i.e., the plan, all the attributes, and
  // the IDs of the columns the scan _actually_ produces)
  const auto &scan_plan = translator_.GetScanPlan();
  std::vector<const planner::AttributeInfo *> ais;
  scan_plan.GetAttributes(ais);
  const auto &output_col_ids = scan_plan.GetColumnIds();

  // 1. Put all the attribute accessors into a vector
  access.clear();
  for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
    access.emplace_back(tile_group_access, ais[output_col_ids[col_idx]]);
  }

  // 2. Add the attribute accessors into the row batch
  for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
    auto *attribute = ais[output_col_ids[col_idx]];
    LOG_TRACE("Adding attribute '%s.%s' (%p) into row batch",
              scan_plan.GetTable()->GetName().c_str(), attribute->name.c_str(),
              attribute);
    batch.AddAttribute(attribute, &access[col_idx]);
  }
}

void TableScanTranslator::ScanConsumer::FilterRowsByVisibility(
    CodeGen &codegen, llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  llvm::Value *txn = translator_.GetCompilationContext().GetTransactionPtr();
  llvm::Value *raw_sel_vec = selection_vector.GetVectorPtr();

  // Invoke TransactionRuntime::PerformRead(...)
  llvm::Value *out_idx =
      codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                   {txn, tile_group_ptr_, tid_start, tid_end, raw_sel_vec});
  selection_vector.SetNumElements(out_idx);
}

// Get the predicate, if one exists
const expression::AbstractExpression *
TableScanTranslator::ScanConsumer::GetPredicate() const {
  return translator_.GetScanPlan().GetPredicate();
}

void TableScanTranslator::ScanConsumer::FilterRowsByPredicate(
    CodeGen &codegen, const TileGroup::TileGroupAccess &access,
    llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  // The batch we're filtering
  auto &compilation_ctx = translator_.GetCompilationContext();
  RowBatch batch{compilation_ctx, tile_group_id_,   tid_start,
                 tid_end,         selection_vector, true};

  // First, check if the predicate is SIMDable
  const auto *predicate = GetPredicate();

  // Determine the attributes the predicate needs
  std::unordered_set<const planner::AttributeInfo *> used_attributes;
  predicate->GetUsedAttributes(used_attributes);

  // Setup the row batch with attribute accessors for the predicate
  std::vector<AttributeAccess> attribute_accessors;
  for (const auto *ai : used_attributes) {
    attribute_accessors.emplace_back(access, ai);
  }
  for (uint32_t i = 0; i < attribute_accessors.size(); i++) {
    auto &accessor = attribute_accessors[i];
    batch.AddAttribute(accessor.GetAttributeRef(), &accessor);
  }

  // Iterate over the batch using a scalar loop
  batch.Iterate(codegen, [&](RowBatch::Row &row) {
    // Evaluate the predicate to determine row validity
    codegen::Value valid_row = row.DeriveValue(codegen, *predicate);

    // Reify the boolean value since it may be NULL
    PL_ASSERT(valid_row.GetType().GetSqlType() == type::Boolean::Instance());
    llvm::Value *bool_val = type::Boolean::Instance().Reify(codegen, valid_row);

    // Set the validity of the row
    row.SetValidity(codegen, bool_val);
  });
}

//===----------------------------------------------------------------------===//
// ATTRIBUTE ACCESS
//===----------------------------------------------------------------------===//

TableScanTranslator::AttributeAccess::AttributeAccess(
    const TileGroup::TileGroupAccess &access, const planner::AttributeInfo *ai)
    : tile_group_access_(access), ai_(ai) {}

codegen::Value TableScanTranslator::AttributeAccess::Access(
    CodeGen &codegen, RowBatch::Row &row) {
  auto raw_row = tile_group_access_.GetRow(row.GetTID(codegen));
  return raw_row.LoadColumn(codegen, ai_->attribute_id);
}

}  // namespace codegen
}  // namespace peloton
