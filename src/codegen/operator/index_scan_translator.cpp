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
  LOG_INFO("Constructing IndexScanTranslator ...");


  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();
  selection_vector_id_ = runtime_state.RegisterState(
    "scanSelVec",
    codegen.ArrayType(codegen.Int32Type(), Vector::kDefaultVectorSize), true);

  LOG_INFO("Finished constructing IndexScanTranslator ...");
}

// Produce!
void IndexScanTranslator::Produce() const {
  printf("producing in index scan translator\n");
  auto &codegen = GetCodeGen();

  const index::ConjunctionScanPredicate* csp = &index_scan_.GetIndexPredicate().GetConjunctionList()[0];
  index::ARTKey continue_key;
  llvm::Value *key_p = codegen.Const64((uint64_t)&continue_key);
  llvm::Value *csp_p = codegen.Const64((uint64_t)csp);

  storage::DataTable &table = *index_scan_.GetTable();
  llvm::Value *catalog_ptr = GetCatalogPtr();
  llvm::Value *db_oid = codegen.Const32(table.GetDatabaseOid());
  llvm::Value *table_oid = codegen.Const32(table.GetOid());
  llvm::Value *table_ptr = codegen.Call(StorageManagerProxy::GetTableWithOid,
                                        {catalog_ptr, db_oid, table_oid});
  llvm::Value *index_oid = codegen.Const32(index_scan_.GetIndex()->GetOid());
  llvm::Value *index_ptr = codegen.Call(StorageManagerProxy::GetIndexWithOid,
                                         {catalog_ptr, db_oid, table_oid, index_oid});

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
    codegen.Call(IndexProxy::CodeGenScan, {index_ptr, csp_p, key_p, tile_group_idx, tile_group_offset});

    auto *not0 = codegen->CreateICmpNE(tile_group_idx, codegen.Const32(0));
    lang::If tile_group_id_not0{codegen, not0};
    {
      // use tile group id and offset to get the tuple!
      llvm::Value *tile_group_ptr = codegen.Call(RuntimeFunctionsProxy::GetTileGroupByGlobalId, {table_ptr, tile_group_idx});

      RowBatch batch{this->GetCompilationContext(), tile_group_idx, tile_group_offset,
                     tile_group_offset, sel_vec, true};

      ConsumerContext context{this->GetCompilationContext(),
                              this->GetPipeline()};
      context.Consume(batch);
    }
    tile_group_id_not0.EndIf();
  }

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

