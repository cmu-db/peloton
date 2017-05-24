//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scan_consumer.h
//
// Identification: src/include/codegen/scan_consumer.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/tile_group.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// An interface for clients to scan a data table. Various callback hooks are
// provided for when the scanner begins iterating over a new tile group, and
// when iteration over a tile group completes. In between these calls,
// ProcessTuples() will be called to allow the client to handle processing of
// all tuples in the provided range of TIDs.
//===----------------------------------------------------------------------===//
class ScanConsumer {
 public:
  // Virtual destructor
  virtual ~ScanConsumer() {}

  // Callback for when iteration begins over a new tile group. The second
  // parameter is a pointer to the tile group.
  virtual void TileGroupStart(CodeGen &codegen,
                              llvm::Value *tile_group_ptr) = 0;

  // Callback to process the tuples with the provided range
  virtual void ProcessTuples(CodeGen &codegen,
                             llvm::Value *tile_group_id,
                             llvm::Value *tid_start, llvm::Value *tid_end,
                             TileGroup::TileGroupAccess &tile_group_access) = 0;

  // Callback for when iteration over the given tile group has completed
  virtual void TileGroupFinish(CodeGen &codegen,
                               llvm::Value *tile_group_ptr) = 0;
};

}  // namespace codegen
}  // namespace peloton