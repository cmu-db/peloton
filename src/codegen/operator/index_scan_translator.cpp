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
#include "codegen/proxy/catalog_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "planner/index_scan_plan.h"
#include "index/art_index.h"
#include "storage/data_table.h"
#include "codegen/proxy/data_table_proxy.h"
#include "index/scan_optimizer.h"
#include "index/art_key.h"
#include "index/art_index.h"
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
IndexScanTranslator::IndexScanTranslator(const planner::IndexScanPlan &index_scan, CompilationContext &context,
                                         Pipeline &pipeline)
  : OperatorTranslator(context, pipeline),
    index_scan_(index_scan)
//  ,
//    index_(*index_scan_.GetIndex().get())
{
  LOG_DEBUG("Constructing IndexScanTranslator ...");


  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();
  selection_vector_id_ = runtime_state.RegisterState(
    "scanSelVec",
    codegen.ArrayType(codegen.Int32Type(), Vector::kDefaultVectorSize), true);

  LOG_DEBUG("Finished constructing IndexScanTranslator ...");
}

// Produce!
void IndexScanTranslator::Produce() const {
  auto &codegen = GetCodeGen();

  const index::ConjunctionScanPredicate* csp = &index_scan_.GetIndexPredicate().GetConjunctionList()[0];

  storage::DataTable &table = *index_scan_.GetTable();
  llvm::Value *catalog_ptr = GetCatalogPtr();
  llvm::Value *db_oid = codegen.Const32(table.GetDatabaseOid());
  llvm::Value *table_oid = codegen.Const32(table.GetOid());
  llvm::Value *table_ptr = codegen.Call(StorageManagerProxy::GetTableWithOid, {catalog_ptr, db_oid, table_oid});
  llvm::Value *index_oid = codegen.Const32(index_scan_.GetIndex()->GetOid());

  llvm::Value *index_ptr = codegen.Call(StorageManagerProxy::GetIndexWithOid,
                                         {catalog_ptr, db_oid, table_oid, index_oid});

  Vector sel_vec{LoadStateValue(selection_vector_id_),
                 Vector::kDefaultVectorSize, codegen.Int32Type()};


  if (csp->IsPointQuery()) {
    llvm::Value *result_p = codegen.Call(RuntimeFunctionsProxy::GetOneResultAndKey, {});
    const storage::Tuple *point_query_key_p = csp->GetPointQueryKey();
    llvm::Value *query_key = codegen.Const64((uint64_t)point_query_key_p);
    codegen.Call(RuntimeFunctionsProxy::ScanKey, {index_ptr, query_key, result_p});
    llvm::Value *valid_id = codegen.Call(RuntimeFunctionsProxy::IsValidTileGroup, {result_p});
    lang::If tile_group_id_valid{codegen, valid_id};
    {
      llvm::Value *tile_group_id = codegen.Call(RuntimeFunctionsProxy::GetTileGroupIdFromResult, {result_p});
      llvm::Value *tile_group_offset = codegen.Call(RuntimeFunctionsProxy::GetTileGroupOffsetFromResult, {result_p});
      const uint32_t num_columns =
        static_cast<uint32_t>(table.GetSchema()->GetColumnCount());
      llvm::Value *column_layouts = codegen->CreateAlloca(
        ColumnLayoutInfoProxy::GetType(codegen), codegen.Const32(num_columns));

      llvm::Value *tile_group_ptr = codegen.Call(RuntimeFunctionsProxy::GetTileGroupByGlobalId,
                                                 {table_ptr, tile_group_id});

      // auto col_layouts = GetColumnLayouts(codegen, tile_group_ptr, column_layouts);
      uint32_t num_cols = table.GetSchema()->GetColumnCount();
      codegen.Call(
        RuntimeFunctionsProxy::GetTileGroupLayout,
        {tile_group_ptr, column_layouts, codegen.Const32(num_cols)});

      // Collect <start, stride, is_columnar> triplets of all columns
      std::vector<TileGroup::ColumnLayout> col_layouts;
      auto *layout_type = ColumnLayoutInfoProxy::GetType(codegen);
      for (uint32_t col_id = 0; col_id < num_cols; col_id++) {
        auto *start = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
          layout_type, column_layouts, col_id, 0));
        auto *stride = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
          layout_type, column_layouts, col_id, 1));
        auto *columnar = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
          layout_type, column_layouts, col_id, 2));
        col_layouts.push_back(TileGroup::ColumnLayout{col_id, start, stride, columnar});
      }

      TileGroup tileGroup(*table.GetSchema());
      TileGroup::TileGroupAccess tile_group_access{tileGroup, col_layouts};

      // visibility
      llvm::Value *txn = this->GetCompilationContext().GetTransactionPtr();
      llvm::Value *raw_sel_vec = sel_vec.GetVectorPtr();

      // Invoke TransactionRuntime::PerformRead(...)
      llvm::Value *out_idx =
        codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                     {txn, tile_group_ptr, tile_group_offset, codegen->CreateAdd(tile_group_offset, codegen.Const32(1)), raw_sel_vec});
      sel_vec.SetNumElements(out_idx);

      // generate the row batch
      RowBatch batch{this->GetCompilationContext(), tile_group_id, codegen.Const32(0),
                     codegen.Const32(1), sel_vec, true};

      std::vector<TableScanTranslator::AttributeAccess> attribute_accesses;
      std::vector<const planner::AttributeInfo *> ais;
      index_scan_.GetAttributes(ais);
      const auto &output_col_ids = index_scan_.GetColumnIds();
      for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
        attribute_accesses.emplace_back(tile_group_access, ais[output_col_ids[col_idx]]);
      }
      for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
        auto *attribute = ais[output_col_ids[col_idx]];
        batch.AddAttribute(attribute, &attribute_accesses[col_idx]);
      }

      ConsumerContext context{this->GetCompilationContext(),
                              this->GetPipeline()};
      context.Consume(batch);
    }
    tile_group_id_valid.EndIf();

    codegen.Call(RuntimeFunctionsProxy::FreeOneResultAndKey, {result_p});
  } else if (csp->IsFullIndexScan()) {

  } else {
    // range scan
    const storage::Tuple *low_key_p = csp->GetLowKey();
    const storage::Tuple *high_key_p = csp->GetHighKey();
    llvm::Value *low_key = codegen.Const64((uint64_t)low_key_p);
    llvm::Value *high_key = codegen.Const64((uint64_t)high_key_p);
    llvm::Value *iterator_ptr = codegen.Call(RuntimeFunctionsProxy::GetIterator, {index_ptr, low_key, high_key});
    codegen.Call(IndexScanIteratorProxy::DoScan, {iterator_ptr});

//    uint32_t batch_size = sel_vec.GetCapacity();
    const uint32_t num_columns = static_cast<uint32_t>(table.GetSchema()->GetColumnCount());
    llvm::Value *column_layouts = codegen->CreateAlloca(ColumnLayoutInfoProxy::GetType(codegen), codegen.Const32(num_columns));
    llvm::Value *result_size = codegen.Call(IndexScanIteratorProxy::GetResultSize, {iterator_ptr});
    llvm::Value *result_iter = codegen.Const64(0);
    lang::Loop loop{codegen,
                    codegen->CreateICmpULT(result_iter, result_size),
                    {{"distinctTileGroupIter", result_iter}}};
    {
      result_iter = loop.GetLoopVar(0);
      llvm::Value *tile_group_id = codegen.Call(IndexScanIteratorProxy::GetTileGroupId, {iterator_ptr, result_iter});
      llvm::Value *tile_group_offset = codegen.Call(IndexScanIteratorProxy::GetTileGroupOffset, {iterator_ptr, result_iter});
      llvm::Value *tile_group_ptr = codegen.Call(RuntimeFunctionsProxy::GetTileGroupByGlobalId, {table_ptr, tile_group_id});

      codegen.Call(RuntimeFunctionsProxy::GetTileGroupLayout, {tile_group_ptr, column_layouts, codegen.Const32(num_columns)});
      // Collect <start, stride, is_columnar> triplets of all columns
      std::vector<TileGroup::ColumnLayout> col_layouts;
      auto *layout_type = ColumnLayoutInfoProxy::GetType(codegen);
      for (uint32_t col_id = 0; col_id < num_columns; col_id++) {
        auto *start = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
          layout_type, column_layouts, col_id, 0));
        auto *stride = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
          layout_type, column_layouts, col_id, 1));
        auto *columnar = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
          layout_type, column_layouts, col_id, 2));
        col_layouts.push_back(TileGroup::ColumnLayout{col_id, start, stride, columnar});
      }

      TileGroup tileGroup(*table.GetSchema());
      TileGroup::TileGroupAccess tile_group_access{tileGroup, col_layouts};


      // visibility
      llvm::Value *txn = this->GetCompilationContext().GetTransactionPtr();
      llvm::Value *raw_sel_vec = sel_vec.GetVectorPtr();
      // Invoke TransactionRuntime::PerformRead(...)
      llvm::Value *out_idx =
        codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                     {txn, tile_group_ptr, tile_group_offset, codegen->CreateAdd(tile_group_offset, codegen.Const32(1)), raw_sel_vec});
      sel_vec.SetNumElements(out_idx);

      // construct the final row batch
      // generate the row batch
      RowBatch final_batch{this->GetCompilationContext(), tile_group_id, codegen.Const32(0),
                           codegen.Const32(1), sel_vec, true};

      std::vector<TableScanTranslator::AttributeAccess> final_attribute_accesses;
      std::vector<const planner::AttributeInfo *> final_ais;
      index_scan_.GetAttributes(final_ais);
      const auto &output_col_ids = index_scan_.GetColumnIds();
      for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
        final_attribute_accesses.emplace_back(tile_group_access, final_ais[output_col_ids[col_idx]]);
      }
      for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
        auto *attribute = final_ais[output_col_ids[col_idx]];
        final_batch.AddAttribute(attribute, &final_attribute_accesses[col_idx]);
      }

      ConsumerContext context{this->GetCompilationContext(),
                              this->GetPipeline()};
      context.Consume(final_batch);


//      llvm::Value *num_tuples = codegen.Call(TileGroupProxy::GetNextTupleSlot, {tile_group_ptr});
//      lang::VectorizedLoop vec_loop{codegen, num_tuples, batch_size, {}};
//      {
//        lang::VectorizedLoop::Range curr_range = vec_loop.GetCurrentRange();
//
//        // Pass the vector to the consumer
//        TileGroup::TileGroupAccess tile_group_access{tileGroup, col_layouts};
//
//        llvm::Value *tid_start = curr_range.start;
//        llvm::Value *tid_end = curr_range.end;
//
//        // visibility
//        llvm::Value *txn = this->GetCompilationContext().GetTransactionPtr();
//        llvm::Value *raw_sel_vec = sel_vec.GetVectorPtr();
//        // Invoke TransactionRuntime::PerformRead(...)
//        llvm::Value *out_idx =
//          codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
//                       {txn, tile_group_ptr, tid_start, tid_end, raw_sel_vec});
//        sel_vec.SetNumElements(out_idx);
//
//        // finally! select the row if it's in the result from range scan
//        auto &compilation_ctx = this->GetCompilationContext();
//        RowBatch batch{compilation_ctx, tile_group_id,   tid_start,
//                       tid_end,         sel_vec, true};
//
//        batch.Iterate(codegen, [&](RowBatch::Row &row) {
//          // Check whether this row in the scan result to determine row validity
//          llvm::Value *bool_val = codegen.Call(IndexScanIteratorProxy::RowOffsetInResult, {iterator_ptr, result_iter, row.GetTID(codegen)});
//
//          // Set the validity of the row
//          row.SetValidity(codegen, bool_val);
//        });
//
//        // construct the final row batch
//        // generate the row batch
//        RowBatch final_batch{this->GetCompilationContext(), tile_group_id, tid_start,
//                             tid_end, sel_vec, true};
//
//        std::vector<TableScanTranslator::AttributeAccess> final_attribute_accesses;
//        std::vector<const planner::AttributeInfo *> final_ais;
//        index_scan_.GetAttributes(final_ais);
//        const auto &output_col_ids = index_scan_.GetColumnIds();
//        for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
//          final_attribute_accesses.emplace_back(tile_group_access, final_ais[output_col_ids[col_idx]]);
//        }
//        for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
//          auto *attribute = final_ais[output_col_ids[col_idx]];
//          final_batch.AddAttribute(attribute, &final_attribute_accesses[col_idx]);
//        }
//
//        ConsumerContext context{this->GetCompilationContext(),
//                                this->GetPipeline()};
//        context.Consume(final_batch);
//
//
//        vec_loop.LoopEnd(codegen, {});
//      }


      // Move to next tile group in the table
      result_iter = codegen->CreateAdd(result_iter, codegen.Const64(1));
      loop.LoopEnd(codegen->CreateICmpULT(result_iter, result_size),
                   {result_iter});
    }
  }

  /*
  llvm::Value *tile_group_idx = codegen.Const64(0);
  llvm::Value *tile_group_offset = codegen.Const64(0);

  Vector sel_vec{LoadStateValue(selection_vector_id_),
                 Vector::kDefaultVectorSize, codegen.Int32Type()};

  // after getting the index, keep making function calls to index.CodeGenScan until there are no new tuples
  lang::Loop loop{codegen,
                  codegen->CreateICmpULT(tile_group_idx, codegen.Const64(-1)),
                  {{"tileGroupIdx", tile_group_idx}}};
  {
    tile_group_idx = loop.GetLoopVar(0);
    codegen.Call(ArtIndexProxy::CodeGenScan, {index_ptr, csp_p, key_p, tile_group_idx, tile_group_offset});

    auto *not0 = codegen->CreateICmpNE(tile_group_idx, codegen.Const32(0));
    lang::If tile_group_id_not0{codegen, not0};
    {
      // use tile group id and offset to get the tuple!
//      llvm::Value *tile_group_ptr = codegen.Call(RuntimeFunctionsProxy::GetTileGroupByGlobalId, {table_ptr, tile_group_idx});

      RowBatch batch{this->GetCompilationContext(), tile_group_idx, tile_group_offset,
                     tile_group_offset, sel_vec, true};

      ConsumerContext context{this->GetCompilationContext(),
                              this->GetPipeline()};
      context.Consume(batch);
    }
    tile_group_id_not0.EndIf();
  }
   */

}


// Get the stringified name of this scan
std::string IndexScanTranslator::GetName() const {
//  std::string name = "Scan('" + GetIndex().GetName() + "'";
//  auto *predicate = GetIndexScanPlan().GetPredicate();
//  if (predicate != nullptr && predicate->IsSIMDable()) {
//    name.append(", ").append(std::to_string(Vector::kDefaultVectorSize));
//  }
//  name.append(")");
  std::string name = "haha";
  return name;
}

//// Index accessor
//const index::ArtIndex &IndexScanTranslator::GetIndex() const {
//  return dynamic_cast<index::ArtIndex &> (*index_scan_.GetIndex().get());
//}
}
}

