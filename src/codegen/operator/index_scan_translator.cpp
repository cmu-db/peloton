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
//  auto &index = GetIndex();
//
//  LOG_INFO("IndexScan on [%s] starting to produce tuples ...", index.GetName().c_str());
  storage::DataTable *table = index_scan_.GetTable();
  llvm::Value *table_ptr = (llvm::Value *)table;
//  llvm::Value *tile_group_ptr = codegen.Call(DataTableProxy::GetTileGroup, {table_ptr, 0});
//  llvm::Value tile_id = (llvm::Value)13;
//  llvm::Value *tile_group_id = (llvm::Value *)13u;
  llvm::Value *tile_group_ptr = codegen.Call(RuntimeFunctionsProxy::GetTileGroupByGlobalId, {table_ptr, codegen.Const32(13)});
  std::vector<llvm::Value *> debug_values;
  debug_values.push_back(tile_group_ptr);
  codegen.CallPrintf("tile group ptr = %d", debug_values);

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

