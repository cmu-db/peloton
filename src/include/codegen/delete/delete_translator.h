//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_translator.h
//
// Identification: src/include/codegen/delete/delete_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/pipeline.h"
#include "planner/delete_plan.h"
#include "codegen/table.h"
#include "storage/tile_group.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a delete operator
//===----------------------------------------------------------------------===//
class DeleteTranslator : public OperatorTranslator {
 public:
  DeleteTranslator(const planner::DeletePlan &delete_plan,
                   CompilationContext &context, Pipeline &pipeline);

  void InitializeState() override {}

  void DefineAuxiliaryFunctions() override {}

  void TearDownState() override {}

  std::string GetName() const override { return "delete"; }

  void Produce() const override;

  void Consume(ConsumerContext &, RowBatch &) const override;
  void Consume(ConsumerContext &, RowBatch::Row &) const override;

 private:
  mutable llvm::Value *table_ptr;
  mutable llvm::Value *tile_group_;
  static bool delete_wrapper(int64_t tile_group_id, uint32_t tuple_id,
                             concurrency::Transaction *txn,
                             storage::DataTable *table,
                             storage::TileGroup *tile_group);
  const planner::DeletePlan &delete_plan_;
  codegen::Table table_;

  //===--------------------------------------------------------------------===//
  // A structure that proxies delete wrapper function calls
  //===--------------------------------------------------------------------===//
  struct _DeleteWrapper {
    // return the mingled function name for function with signature
    // peloton::codegen::DeleteTranslator::wrapper(unsigned int,
    //                                             unsigned int,
    //                                             peloton::concurrency::Transaction*,
    //                                             peloton::storage::DataTable*)
    static const std::string &GetFunctionName();
    // return the llvm function bound with the above function name.
    // perform binding if no such function is found
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};


}  // namespace codegen
}  // namespace peloton
