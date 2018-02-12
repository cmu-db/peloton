//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator.h
//
// Identification: src/include/codegen/operator/table_scan_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/scan_callback.h"
#include "codegen/table.h"

namespace peloton {

namespace planner {
class SeqScanPlan;
}  // namespace planner

namespace storage {
class DataTable;
}  // namespace storage

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for table scans
//===----------------------------------------------------------------------===//
class TableScanTranslator : public OperatorTranslator {
 public:
  // Constructor
  TableScanTranslator(const planner::SeqScanPlan &scan,
                      CompilationContext &context, Pipeline &pipeline);

  void InitializeQueryState() override {}

  // Table scans don't rely on any auxiliary functions
  void DefineAuxiliaryFunctions() override {}

  // The method that produces new tuples
  void Produce() const override;

  // Scans are leaves in the query plan and, hence, do not consume tuples
  void Consume(ConsumerContext &, RowBatch &) const override {}
  void Consume(ConsumerContext &, RowBatch::Row &) const override {}

  // Similar to InitializeQueryState(), table scans don't have any state
  void TearDownQueryState() override {}

 private:
  void ProduceSerial() const;

  void ProduceParallel() const;

  //===--------------------------------------------------------------------===//
  // An attribute accessor that uses the backing tile group to access columns
  //===--------------------------------------------------------------------===//
  class AttributeAccess : public RowBatch::AttributeAccess {
   public:
    // Constructor
    AttributeAccess(const TileGroup::TileGroupAccess &access,
                    const planner::AttributeInfo *ai);

    // Access an attribute in the given row
    codegen::Value Access(CodeGen &codegen, RowBatch::Row &row) override;

    const planner::AttributeInfo *GetAttributeRef() const { return ai_; }

   private:
    // The accessor we use to load column values
    const TileGroup::TileGroupAccess &tile_group_access_;
    // The attribute we will access
    const planner::AttributeInfo *ai_;
  };

  //===--------------------------------------------------------------------===//
  // The class responsible for generating vectorized scan code over tile groups
  //===--------------------------------------------------------------------===//
  class ScanConsumer : public codegen::ScanCallback {
   public:
    // Constructor
    ScanConsumer(ConsumerContext &ctx, const planner::SeqScanPlan &plan,
                 Vector &selection_vector);

    // The callback when starting iteration over a new tile group
    void TileGroupStart(CodeGen &, llvm::Value *tile_group_id,
                        llvm::Value *tile_group_ptr) override {
      tile_group_id_ = tile_group_id;
      tile_group_ptr_ = tile_group_ptr;
    }

    // The code that forms the body of the scan loop
    void ProcessTuples(CodeGen &codegen, llvm::Value *tid_start,
                       llvm::Value *tid_end,
                       TileGroup::TileGroupAccess &tile_group_access) override;

    // The callback when finishing iteration over a tile group
    void TileGroupFinish(CodeGen &, llvm::Value *) override {}

   private:
    void SetupRowBatch(RowBatch &batch,
                       TileGroup::TileGroupAccess &tile_group_access,
                       std::vector<AttributeAccess> &access) const;

    void FilterRowsByVisibility(CodeGen &codegen, llvm::Value *tid_start,
                                llvm::Value *tid_end,
                                Vector &selection_vector) const;

    // Filter all the rows whose TIDs are in the range [tid_start, tid_end] and
    // store their TIDs in the output TID selection vector
    void FilterRowsByPredicate(CodeGen &codegen,
                               const TileGroup::TileGroupAccess &access,
                               llvm::Value *tid_start, llvm::Value *tid_end,
                               Vector &selection_vector) const;

   private:
    // The consumer context
    ConsumerContext &ctx_;
    // The plan node
    const planner::SeqScanPlan &plan_;
    // The selection vector used for vectorized scans
    Vector &selection_vector_;
    // The current tile group id we're scanning over
    llvm::Value *tile_group_id_;
    // The current tile group we're scanning over
    llvm::Value *tile_group_ptr_;
  };

  // Plan accessor
  const planner::SeqScanPlan &GetScanPlan() const;

 private:
  // The code-generating table instance
  codegen::Table table_;
};

}  // namespace codegen
}  // namespace peloton