//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator.cpp
//
// Identification: src/codegen/table_scan_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/table_scan_translator.h"

#include "codegen/if.h"
#include "codegen/catalog_proxy.h"
//#include "common/simd_util.h"

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

  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();
  selection_vector_id_ = runtime_state.RegisterState(
      "scanSelVec",
      codegen.VectorType(codegen.Int32Type(), Vector::kDefaultVectorSize),
      true);

  LOG_DEBUG("Finished constructing TableScanTranslator ...");
}

// Produce!
void TableScanTranslator::Produce() const {
  auto &codegen = GetCodeGen();
  auto &table = GetTable();

  LOG_DEBUG("TableScan on [%u] starting to produce tuples ...", table.GetOid());

  // Get the table instance from the database
  llvm::Value *catalog_ptr = GetCatalogPtr();
  llvm::Value *table_ptr =
      codegen.CallFunc(CatalogProxy::_GetTableWithOid::GetFunction(codegen),
                       {catalog_ptr, codegen.Const32(table.GetDatabaseOid()),
                        codegen.Const32(table.GetOid())});

  // The output buffer for the scan
  Vector selection_vector{LoadStateValue(selection_vector_id_),
                          Vector::kDefaultVectorSize, codegen.Int32Type()};

  // Do the vectorized scan
  ScanConsumer scan_consumer{*this, selection_vector};
  table_.GenerateVectorizedScan(codegen, table_ptr,
                                selection_vector.GetCapacity(), scan_consumer);

  LOG_DEBUG("TableScan on [%u] finished producing tuples ...", table.GetOid());
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

//===----------------------------------------------------------------------===//
// VECTORIZED SCAN CONSUMER
//===----------------------------------------------------------------------===//

// Constructor
TableScanTranslator::ScanConsumer::ScanConsumer(
    const TableScanTranslator &translator, Vector &selection_vector)
    : translator_(translator), selection_vector_(selection_vector) {}

// Generate the body of the vectorized scan
void TableScanTranslator::ScanConsumer::ScanBody(
    CodeGen &, llvm::Value *tid_start, llvm::Value *tid_end,
    TileGroup::TileGroupAccess &tile_group_access) {
  auto *predicate = GetPredicate();
  if (predicate != nullptr) {
    // First perform a vectorized filter, putting TIDs into the selection vector
    FilterRows(tile_group_access, tid_start, tid_end, selection_vector_);
  }

  // 2. Setup the row batch
  RowBatch batch{translator_.GetCompilationContext(), tid_start, tid_end,
                 selection_vector_, predicate != nullptr};

  std::vector<TableScanTranslator::AttributeAccess> attribute_accesses;
  SetupRowBatch(batch, tile_group_access, attribute_accesses);

  // Push the batch into the pipeline
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
    LOG_DEBUG("Putting AI %p [table: %u] into context", attribute,
              translator_.GetTable().GetOid());
    batch.AddAttribute(attribute, &access[col_idx]);
  }
}

const expression::AbstractExpression *
TableScanTranslator::ScanConsumer::GetPredicate() const {
  return translator_.GetScanPlan().GetPredicate();
}

void TableScanTranslator::ScanConsumer::FilterRows(
    const TileGroup::TileGroupAccess &access, llvm::Value *tid_start,
    llvm::Value *tid_end, Vector &selection_vector) const {
  auto &codegen = translator_.GetCodeGen();

  // The batch we're filtering
  auto &compilation_ctx = translator_.GetCompilationContext();
  RowBatch batch{compilation_ctx, tid_start, tid_end, selection_vector, false};

  // First, check if the predicate is SIMDable
  const auto *predicate = GetPredicate();

  if (predicate->IsSIMDable()) {
    // The predicate is SIMDable, do it here
    /*
    SIMDFilterRows(batch, access);
    return;
    */
  }

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

    // Set the validity of the row
    row.SetValidity(codegen, valid_row.GetValue());
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