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

#include "codegen/lang/if.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/zone_map_proxy.h"
#include "codegen/type/boolean_type.h"
#include "planner/seq_scan_plan.h"
#include "storage/data_table.h"
#include "storage/zone_map_manager.h"

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
  LOG_DEBUG("Constructing TableScanTranslator ...");

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
  LOG_DEBUG("Finished constructing TableScanTranslator ...");
}

// Produce!
void TableScanTranslator::Produce() const {
  auto &codegen = GetCodeGen();
  auto &table = GetTable();

  LOG_TRACE("TableScan on [%u] starting to produce tuples ...", table.GetOid());

  // Get the table instance from the database
  llvm::Value *storage_manager_ptr = GetStorageManagerPtr();
  llvm::Value *db_oid = codegen.Const32(table.GetDatabaseOid());
  llvm::Value *table_oid = codegen.Const32(table.GetOid());
  llvm::Value *table_ptr =
      codegen.Call(StorageManagerProxy::GetTableWithOid,
                   {storage_manager_ptr, db_oid, table_oid});

  // The selection vector for the scan
  auto *raw_vec = codegen.AllocateBuffer(
      codegen.Int32Type(), Vector::kDefaultVectorSize, "scanSelVector");
  Vector sel_vec{raw_vec, Vector::kDefaultVectorSize, codegen.Int32Type()};

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
  ScanConsumer scan_consumer{*this, sel_vec};
  table_.GenerateScan(codegen, table_ptr, sel_vec.GetCapacity(), scan_consumer,
                      predicate_ptr, num_preds);
  LOG_TRACE("TableScan on [%u] finished producing tuples ...", table.GetOid());
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
  llvm::Value *executor_context_ptr =
      translator_.GetCompilationContext().GetExecutorContextPtr();
  llvm::Value *txn = codegen.Call(ExecutorContextProxy::GetTransaction,
                                  {executor_context_ptr});
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
  LOG_DEBUG("Is Predicate SIMDable : %d", predicate->IsSIMDable());
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
