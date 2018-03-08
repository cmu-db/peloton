//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator.cpp
//
// Identification: src/codegen/operator/table_scan_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/table_scan_translator.h"

#include "codegen/lang/if.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/proxy/zone_map_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/vector.h"
#include "planner/seq_scan_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// Table Scan Translator
///
////////////////////////////////////////////////////////////////////////////////

TableScanTranslator::TableScanTranslator(const planner::SeqScanPlan &scan,
                                         CompilationContext &context,
                                         Pipeline &pipeline)
    : OperatorTranslator(scan, context, pipeline), table_(*scan.GetTable()) {
  // Set ourselves as the source of the pipeline
  pipeline.MarkSource(this, Pipeline::Parallelism::Serial);
  // If there is a predicate, prepare a translator for it
  const auto *predicate = scan.GetPredicate();
  if (predicate != nullptr) {
    context.Prepare(*predicate);
  }
}

// TODO merge serial and parallel ...
// TODO cleanup zonemap stuff

llvm::Value *TableScanTranslator::LoadTablePtr(CodeGen &codegen) const {
  const storage::DataTable &table = *GetScanPlan().GetTable();

  // Get the table instance from the database
  llvm::Value *db_oid = codegen.Const32(table.GetDatabaseOid());
  llvm::Value *table_oid = codegen.Const32(table.GetOid());
  return codegen.Call(StorageManagerProxy::GetTableWithOid,
                      {GetStorageManagerPtr(), db_oid, table_oid});
}

void TableScanTranslator::ProduceSerial() const {
  auto producer = [this](ConsumerContext &ctx) {
    CodeGen &codegen = GetCodeGen();

    // Load the table
    auto *table_ptr = LoadTablePtr(codegen);

    // The selection vector for the scan
    auto *i32_type = codegen.Int32Type();
    auto vec_size = Vector::kDefaultVectorSize.load();
    auto *raw_vec = codegen.AllocateBuffer(i32_type, vec_size, "scanPosList");
    Vector position_list{raw_vec, vec_size, i32_type};

    auto predicate = const_cast<expression::AbstractExpression *>(
        GetScanPlan().GetPredicate());
    llvm::Value *predicate_ptr = codegen->CreateIntToPtr(
        codegen.Const64((int64_t)predicate),
        AbstractExpressionProxy::GetType(codegen)->getPointerTo());
    size_t num_preds = 0;

    auto *zone_map_manager = storage::ZoneMapManager::GetInstance();
    if (predicate != nullptr && zone_map_manager->ZoneMapTableExists()) {
      if (predicate->IsZoneMappable()) {
        num_preds = predicate->GetNumberofParsedPredicates();
      }
    }

    ScanConsumer scan_consumer{ctx, GetScanPlan(), position_list};
    table_.GenerateScan(codegen, table_ptr, nullptr, nullptr, vec_size,
                        predicate_ptr, num_preds, scan_consumer);
  };

  // Execute serially
  GetPipeline().RunSerial(producer);
}

void TableScanTranslator::ProduceParallel() const {
  CodeGen &codegen = GetCodeGen();

  // The input arguments
  const storage::DataTable &table = *GetScanPlan().GetTable();

  // The use RuntimeFunctions::ExecuteTableScan() to launch a parallel scan.
  // We pass in the database and table IDs to scan the correct table.
  auto *dispatcher =
      RuntimeFunctionsProxy::ExecuteTableScan.GetFunction(codegen);
  std::vector<llvm::Value *> dispatch_args = {
      codegen.Const32(table.GetDatabaseOid()), codegen.Const32(table.GetOid())};

  // Our function needs to know the start and stop tile groups to scan
  std::vector<llvm::Type *> pipeline_arg_types = {codegen.Int64Type(),
                                                  codegen.Int64Type()};

  // Parallel production
  auto producer = [this, &codegen, &table](
      ConsumerContext &ctx, const std::vector<llvm::Value *> params) {
    PELOTON_ASSERT(params.size() == 2);
    llvm::Value *tilegroup_start = params[0];
    llvm::Value *tilegroup_end = params[1];

    // Load the table pointer
    auto *table_ptr = LoadTablePtr(codegen);

    // The selection vector for the scan
    auto *i32_type = codegen.Int32Type();
    auto vec_size = Vector::kDefaultVectorSize.load();
    auto *raw_vec = codegen.AllocateBuffer(i32_type, vec_size, "scanPosList");
    Vector position_list{raw_vec, vec_size, i32_type};

    // zonemap
    auto predicate = const_cast<expression::AbstractExpression *>(
        GetScanPlan().GetPredicate());
    llvm::Value *predicate_ptr = codegen->CreateIntToPtr(
        codegen.Const64((int64_t)predicate),
        AbstractExpressionProxy::GetType(codegen)->getPointerTo());
    size_t num_preds = 0;

    auto *zone_map_manager = storage::ZoneMapManager::GetInstance();
    if (predicate != nullptr && zone_map_manager->ZoneMapTableExists()) {
      if (predicate->IsZoneMappable()) {
        num_preds = predicate->GetNumberofParsedPredicates();
      }
    }

    // Scan the given range of the table
    ScanConsumer scan_consumer{ctx, GetScanPlan(), position_list};
    table_.GenerateScan(codegen, table_ptr, tilegroup_start, tilegroup_end,
                        vec_size, predicate_ptr, num_preds, scan_consumer);
  };

  // Execute parallel
  auto &pipeline = GetPipeline();
  pipeline.RunParallel(dispatcher, dispatch_args, pipeline_arg_types, producer);
}

void TableScanTranslator::Produce() const {
  if (GetPipeline().IsParallel()) {
    ProduceParallel();
  } else {
    ProduceSerial();
  }
}

const planner::SeqScanPlan &TableScanTranslator::GetScanPlan() const {
  return GetPlanAs<planner::SeqScanPlan>();
}

////////////////////////////////////////////////////////////////////////////////
///
/// Scan Consumer
///
////////////////////////////////////////////////////////////////////////////////

TableScanTranslator::ScanConsumer::ScanConsumer(
    ConsumerContext &ctx, const planner::SeqScanPlan &plan,
    Vector &selection_vector)
    : ctx_(ctx), plan_(plan), selection_vector_(selection_vector) {}

// Generate the body of the vectorized scan
void TableScanTranslator::ScanConsumer::ProcessTuples(
    CodeGen &codegen, llvm::Value *tid_start, llvm::Value *tid_end,
    TileGroup::TileGroupAccess &tile_group_access) {
  // 1. Filter the rows in the range [tid_start, tid_end) by txn visibility
  FilterRowsByVisibility(codegen, tid_start, tid_end, selection_vector_);

  // 2. Filter rows by the given predicate (if one exists)
  auto *predicate = plan_.GetPredicate();
  if (predicate != nullptr) {
    // First perform a vectorized filter, putting TIDs into the selection vector
    FilterRowsByPredicate(codegen, tile_group_access, tid_start, tid_end,
                          selection_vector_);
  }

  // 3. Setup the (filtered) row batch and setup attribute accessors
  RowBatch batch{ctx_.GetCompilationContext(), tile_group_id_, tid_start,
                 tid_end, selection_vector_, true};

  std::vector<TableScanTranslator::AttributeAccess> attribute_accesses;
  SetupRowBatch(batch, tile_group_access, attribute_accesses);

  // 4. Push the batch into the pipeline
  ctx_.Consume(batch);
}

void TableScanTranslator::ScanConsumer::SetupRowBatch(
    RowBatch &batch, TileGroup::TileGroupAccess &tile_group_access,
    std::vector<TableScanTranslator::AttributeAccess> &access) const {
  // Grab a hold of the stuff we need (i.e., the plan, all the attributes, and
  // the IDs of the columns the scan _actually_ produces)
  std::vector<const planner::AttributeInfo *> ais;
  plan_.GetAttributes(ais);
  const auto &output_col_ids = plan_.GetColumnIds();

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
  ExecutionConsumer &ec = ctx_.GetCompilationContext().GetExecutionConsumer();
  llvm::Value *txn = ec.GetTransactionPtr(ctx_.GetCompilationContext());
  llvm::Value *raw_sel_vec = selection_vector.GetVectorPtr();

  // Invoke TransactionRuntime::PerformRead(...)
  llvm::Value *out_idx =
      codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                   {txn, tile_group_ptr_, tid_start, tid_end, raw_sel_vec});
  selection_vector.SetNumElements(out_idx);
}

void TableScanTranslator::ScanConsumer::FilterRowsByPredicate(
    CodeGen &codegen, const TileGroup::TileGroupAccess &access,
    llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  // The batch we're filtering
  RowBatch batch{ctx_.GetCompilationContext(), tile_group_id_, tid_start,
                 tid_end, selection_vector, true};

  // Determine the attributes the predicate needs
  const auto *predicate = plan_.GetPredicate();

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
    PELOTON_ASSERT(valid_row.GetType().GetSqlType() ==
                   type::Boolean::Instance());
    llvm::Value *bool_val = type::Boolean::Instance().Reify(codegen, valid_row);

    // Set the validity of the row
    row.SetValidity(codegen, bool_val);
  });
}

////////////////////////////////////////////////////////////////////////////////
///
/// Attribute Access
///
////////////////////////////////////////////////////////////////////////////////

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
