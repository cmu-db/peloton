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

#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/operator/index_scan_translator.h"

#include "codegen/lang/if.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "planner/index_scan_plan.h"
#include "storage/data_table.h"
#include "codegen/proxy/data_table_proxy.h"
#include "index/scan_optimizer.h"
#include "codegen/proxy/index_proxy.h"
#include "codegen/operator/table_scan_translator.h"
#include "codegen/proxy/index_scan_iterator_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/lang/vectorized_loop.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// INDEX SCAN TRANSLATOR
//===----------------------------------------------------------------------===//

// Constructor
IndexScanTranslator::IndexScanTranslator(
    const planner::IndexScanPlan &index_scan, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      index_scan_(index_scan)
//  ,
//    index_(*index_scan_.GetIndex().get())
{
  LOG_DEBUG("Constructing IndexScanTranslator ...");

  // The restriction, if one exists
  const auto *predicate = index_scan_.GetPredicate();

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

  LOG_DEBUG("Finished constructing IndexScanTranslator ...");
}

// Produce!
void IndexScanTranslator::Produce() const {
  LOG_INFO("IndexScanTranslator %s start producing\n", GetName().c_str());
  auto &codegen = GetCodeGen();

  const index::ConjunctionScanPredicate *csp =
      &index_scan_.GetIndexPredicate().GetConjunctionList()[0];

  // get pointer to data table and pointer to index
  storage::DataTable &table = *index_scan_.GetTable();
  llvm::Value *storage_manager_ptr = GetStorageManagerPtr();
  llvm::Value *db_oid = codegen.Const32(table.GetDatabaseOid());
  llvm::Value *table_oid = codegen.Const32(table.GetOid());
  llvm::Value *table_ptr = codegen.Call(StorageManagerProxy::GetTableWithOid,
                                        {storage_manager_ptr, db_oid, table_oid});
  llvm::Value *index_oid = codegen.Const32(index_scan_.GetIndex()->GetOid());

  llvm::Value *index_ptr =
      codegen.Call(StorageManagerProxy::GetIndexWithOid,
                   {storage_manager_ptr, db_oid, table_oid, index_oid});

  Vector sel_vec{LoadStateValue(selection_vector_id_),
                 Vector::kDefaultVectorSize, codegen.Int32Type()};

  // get query keys in the ConjunctionScanPredicate in index scan plan node
  llvm::Value *point_key = codegen.Const64(0);
  llvm::Value *low_key = codegen.Const64(0);
  llvm::Value *high_key = codegen.Const64(0);
  if (csp->IsPointQuery()) {
    point_key = codegen.Const64((uint64_t)(csp->GetPointQueryKey()));
  } else if (!csp->IsFullIndexScan()) {
    // range scan
    low_key = codegen.Const64((uint64_t)(csp->GetLowKey()));
    high_key = codegen.Const64((uint64_t)(csp->GetHighKey()));
  }

  // construct an iterator for code gen index scan
  llvm::Value *iterator_ptr =
      codegen.Call(RuntimeFunctionsProxy::GetIterator,
                   {index_ptr, point_key, low_key, high_key});
  // the iterator makes function call to the index
  codegen.Call(IndexScanIteratorProxy::DoScan, {iterator_ptr});

  const uint32_t num_columns =
      static_cast<uint32_t>(table.GetSchema()->GetColumnCount());
  llvm::Value *column_layouts = codegen->CreateAlloca(
      ColumnLayoutInfoProxy::GetType(codegen), codegen.Const32(num_columns));

  // get the index scan result size and iterator over all the results
  llvm::Value *result_size =
      codegen.Call(IndexScanIteratorProxy::GetResultSize, {iterator_ptr});
  llvm::Value *result_iter = codegen.Const64(0);
  lang::Loop loop{codegen,
                  codegen->CreateICmpULT(result_iter, result_size),
                  {{"distinctTileGroupIter", result_iter}}};
  {
    result_iter = loop.GetLoopVar(0);

    // get pointer to the target tile group
    llvm::Value *tile_group_id = codegen.Call(
        IndexScanIteratorProxy::GetTileGroupId, {iterator_ptr, result_iter});
    llvm::Value *tile_group_offset =
        codegen.Call(IndexScanIteratorProxy::GetTileGroupOffset,
                     {iterator_ptr, result_iter});
    llvm::Value *tile_group_ptr =
        codegen.Call(RuntimeFunctionsProxy::GetTileGroupByGlobalId,
                     {table_ptr, tile_group_id});

    codegen.Call(
        RuntimeFunctionsProxy::GetTileGroupLayout,
        {tile_group_ptr, column_layouts, codegen.Const32(num_columns)});
    // Collect <start, stride, is_columnar> triplets of all columns
    std::vector<TileGroup::ColumnLayout> col_layouts;
    auto *layout_type = ColumnLayoutInfoProxy::GetType(codegen);
    printf("num_columns = %d\n", num_columns);
    for (uint32_t col_id = 0; col_id < num_columns; col_id++) {
      auto *start = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
          layout_type, column_layouts, col_id, 0));
      auto *stride = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
          layout_type, column_layouts, col_id, 1));
      auto *columnar = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
          layout_type, column_layouts, col_id, 2));
      col_layouts.push_back(
          TileGroup::ColumnLayout{col_id, start, stride, columnar});
    }

    TileGroup tileGroup(*table.GetSchema());
    TileGroup::TileGroupAccess tile_group_access{tileGroup, col_layouts};

    // visibility
    llvm::Value *txn = this->GetCompilationContext().GetTransactionPtr();
    llvm::Value *raw_sel_vec = sel_vec.GetVectorPtr();
    // Invoke TransactionRuntime::PerformRead(...)
    llvm::Value *out_idx =
        codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                     {txn, tile_group_ptr, tile_group_offset,
                      codegen->CreateAdd(tile_group_offset, codegen.Const32(1)),
                      raw_sel_vec});
    sel_vec.SetNumElements(out_idx);

    // filter by predicate
    const auto *predicate = index_scan_.GetPredicate();
    if (predicate != nullptr) {
      RowBatch batch{this->GetCompilationContext(), tile_group_id,
                           codegen.Const32(0), codegen.Const32(1), sel_vec, true};
      // Determine the attributes the predicate needs
      std::unordered_set<const planner::AttributeInfo *> used_attributes;
      predicate->GetUsedAttributes(used_attributes);
      printf("used_attributes size = %lu\n", used_attributes.size());


      // Setup the row batch with attribute accessors for the predicate
      std::vector<TableScanTranslator::AttributeAccess> attribute_accessors;
      for (const auto *ai : used_attributes) {
        printf("used attributed name = %s\n", ai->name.c_str());
        attribute_accessors.emplace_back(tile_group_access, ai);
      }
      for (uint32_t i = 0; i < attribute_accessors.size(); i++) {
        auto &accessor = attribute_accessors[i];
        printf("attribute accessor name = %s\n", accessor.GetAttributeRef()->name.c_str());
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
    printf("good after filtering by predicate\n");

    // construct the final row batch
    // one tuple per row batch
    RowBatch final_batch{this->GetCompilationContext(), tile_group_id,
                         codegen.Const32(0), codegen.Const32(1), sel_vec, true};

    std::vector<TableScanTranslator::AttributeAccess> final_attribute_accesses;
    std::vector<const planner::AttributeInfo *> final_ais;
    index_scan_.GetAttributes(final_ais);
    printf("final_ais size = %lud\n", final_ais.size());
    std::vector<oid_t> output_col_ids;
    if (index_scan_.GetColumnIds().size() != 0) {
      output_col_ids = index_scan_.GetColumnIds();
    } else {
      output_col_ids.resize(table.GetSchema()->GetColumnCount());
      std::iota(output_col_ids.begin(), output_col_ids.end(), 0);
    }
//    const auto &output_col_ids = index_scan_.GetColumnIds();

    printf("output column size = %lu\n", output_col_ids.size());
    for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
      final_attribute_accesses.emplace_back(tile_group_access,
                                            final_ais[output_col_ids[col_idx]]);
    }
    for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
      auto *attribute = final_ais[output_col_ids[col_idx]];
      final_batch.AddAttribute(attribute, &final_attribute_accesses[col_idx]);
    }


    ConsumerContext context{this->GetCompilationContext(), this->GetPipeline()};
    context.Consume(final_batch);

    // Move to next tuple in the index scan result
    result_iter = codegen->CreateAdd(result_iter, codegen.Const64(1));
    loop.LoopEnd(codegen->CreateICmpULT(result_iter, result_size),
                 {result_iter});
  }

  // free the memory allocated for the index scan iterator
  codegen.Call(RuntimeFunctionsProxy::DeleteIterator, {iterator_ptr});
}

// Get the name of this scan
std::string IndexScanTranslator::GetName() const {
  std::string name = "Scan('" + GetIndex().GetName() + "'";
  auto *predicate = GetIndexScanPlan().GetPredicate();
  if (predicate != nullptr && predicate->IsSIMDable()) {
    name.append(", ").append(std::to_string(Vector::kDefaultVectorSize));
  }
  name.append(")");
  return name;
}

// Index accessor
const index::Index &IndexScanTranslator::GetIndex() const {
  return dynamic_cast<index::Index &>(*index_scan_.GetIndex().get());
}
}
}
